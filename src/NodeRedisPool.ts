/**
 * Scoped node-redis connections for standalone Redis and Redis Sentinel.
 *
 * Producer, worker, and maintenance traffic use independent bounded pools.
 * Redis Cluster is deliberately unsupported because EffectMQ's atomic scripts
 * span several keys that are not guaranteed to share a cluster hash slot.
 *
 * @module
 */
import { Context, Data, Effect, Layer, Metric, Ref, Scope } from "effect";
import * as Redis from "effect/unstable/persistence/Redis";
import {
  createClientPool,
  createSentinel,
  RESP_TYPES,
  type RedisClientOptions,
  type RedisPoolOptions as NodeRedisClientPoolOptions,
  type RedisSentinelOptions,
} from "redis";
import * as Observability from "./Observability.js";
import {
  makeConnectionRoles,
  make as makeRedisPool,
  RedisConnectionRoles,
  RedisPool,
} from "./RedisPool.js";

export type RedisPoolOptions = Omit<
  RedisClientOptions,
  "clientSideCache" | "RESP"
> & {
  /** RESP2 and RESP3 are both covered by the compatibility suite. */
  readonly RESP?: 2 | 3;
};
export type RedisRole = "producer" | "worker" | "maintenance";

/** Explicitly bounded node-redis pool settings. */
export interface BoundedPoolOptions {
  readonly minimum?: number;
  readonly maximum?: number;
  readonly acquireTimeout?: number;
  readonly cleanupDelay?: number;
}

/** Standalone configuration. Legacy node-redis options remain source-compatible. */
export type StandaloneRedisConfig = RedisPoolOptions & {
  readonly topology?: "standalone";
  readonly pool?: BoundedPoolOptions;
};

/** Sentinel discovers and reconnects to the current writable primary. */
export interface SentinelRedisConfig {
  readonly topology: "sentinel";
  readonly sentinel: RedisSentinelOptions;
}

/** Accepted only so unsupported production configuration fails as typed data. */
export interface ClusterRedisConfig {
  readonly topology: "cluster";
}

export type RedisConfig =
  | StandaloneRedisConfig
  | SentinelRedisConfig
  | ClusterRedisConfig;

export class UnsupportedRedisTopology extends Data.TaggedError(
  "UnsupportedRedisTopology",
)<{
  readonly topology: "cluster";
  readonly reason: string;
}> {}

export class InvalidRedisConfiguration extends Data.TaggedError(
  "InvalidRedisConfiguration",
)<{ readonly reason: string }> {}

export interface RedisRoleHealth {
  readonly state: "disconnected" | "connecting" | "ready" | "degraded";
  readonly commandErrors: number;
  readonly reconnects: number;
  readonly lastChangeAt: number;
}

export interface RedisHealthSnapshot {
  readonly topology: "standalone" | "sentinel";
  readonly ready: boolean;
  readonly roles: Readonly<Record<RedisRole, RedisRoleHealth>>;
}

export interface RedisConnectionHealthService {
  readonly snapshot: Effect.Effect<RedisHealthSnapshot>;
  readonly readiness: Effect.Effect<boolean>;
}

/** Secret-free passive readiness and command-health state. */
export class RedisConnectionHealth extends Context.Service<
  RedisConnectionHealth,
  RedisConnectionHealthService
>()("effectmq/RedisConnectionHealth") {}

const roles: ReadonlyArray<RedisRole> = ["producer", "worker", "maintenance"];

const initialRoleHealth = (): RedisRoleHealth => ({
  state: "disconnected",
  commandErrors: 0,
  reconnects: 0,
  lastChangeAt: Date.now(),
});

const binaryTypeMapping = {
  [RESP_TYPES.BLOB_STRING]: Buffer,
  [RESP_TYPES.MAP]: Map,
};

const toArg = (arg: string | Uint8Array) =>
  typeof arg === "string" || Buffer.isBuffer(arg) ? arg : Buffer.from(arg);

const configurationError = (reason: string) =>
  new InvalidRedisConfiguration({ reason });

const validatePool = (options: BoundedPoolOptions | undefined) => {
  const minimum = options?.minimum ?? 1;
  const maximum = options?.maximum ?? 100;
  const acquireTimeout = options?.acquireTimeout ?? 3_000;
  const cleanupDelay = options?.cleanupDelay ?? 3_000;
  if (!Number.isInteger(minimum) || minimum < 1)
    return configurationError("pool.minimum must be an integer >= 1");
  if (!Number.isInteger(maximum) || maximum < minimum || maximum > 1_000)
    return configurationError(
      "pool.maximum must be an integer between minimum and 1000",
    );
  if (!Number.isFinite(acquireTimeout) || acquireTimeout < 0)
    return configurationError("pool.acquireTimeout must be >= 0");
  if (!Number.isFinite(cleanupDelay) || cleanupDelay < 0)
    return configurationError("pool.cleanupDelay must be >= 0");
  return undefined;
};

const unsupportedCluster = () =>
  new UnsupportedRedisTopology({
    topology: "cluster",
    reason:
      "EffectMQ requires multi-key atomic scripts and currently supports standalone Redis or Sentinel only",
  });

type HealthState = Record<RedisRole, RedisRoleHealth>;

const updateHealth = (
  health: Ref.Ref<HealthState>,
  role: RedisRole,
  update: (current: RedisRoleHealth) => RedisRoleHealth,
) =>
  Ref.update(health, (current) => ({
    ...current,
    [role]: update(current[role]),
  }));

const makeClient = Effect.fnUntraced(function* (
  config: Exclude<RedisConfig, ClusterRedisConfig>,
  role: RedisRole,
  health: Ref.Ref<HealthState>,
) {
  const topology = config.topology ?? "standalone";
  const standalone = topology === "standalone";
  const standaloneClient =
    config.topology !== "sentinel"
      ? (() => {
          const { topology: _topology, pool, ...clientOptions } = config;
          return createClientPool(
            clientOptions as Omit<RedisClientOptions, "clientSideCache">,
            pool as Partial<NodeRedisClientPoolOptions> | undefined,
          );
        })()
      : undefined;
  const sentinelClient =
    config.topology === "sentinel"
      ? createSentinel(config.sentinel)
      : undefined;
  const client = standaloneClient ?? sentinelClient;
  if (client === undefined) throw new Error("unreachable Redis topology");

  // EventEmitter treats an unhandled error event as process-fatal. Install
  // before connect and log no connection config or raw error string.
  client.on("error", () => {
    Effect.runFork(
      Effect.all([
        updateHealth(health, role, (current) => ({
          ...current,
          state: "degraded",
          commandErrors: current.commandErrors + 1,
          lastChangeAt: Date.now(),
        })),
        Effect.logError("effectmq Redis connection error", { role, topology }),
        Metric.update(Observability.redisErrors, 1),
      ]).pipe(Effect.asVoid),
    );
  });

  if (standalone) {
    client.on("reconnecting", () => {
      Effect.runFork(
        Effect.all([
          updateHealth(health, role, (current) => ({
            ...current,
            state: "connecting",
            reconnects: current.reconnects + 1,
            lastChangeAt: Date.now(),
          })),
          Effect.logWarning("effectmq Redis connection reconnecting", { role }),
          Metric.update(Observability.redisReconnects, 1),
        ]).pipe(Effect.asVoid),
      );
    });
  } else {
    client.on("topology-change", (event) => {
      Effect.runFork(
        Effect.all([
          updateHealth(health, role, (current) => ({
            ...current,
            state: "connecting",
            reconnects: current.reconnects + 1,
            lastChangeAt: Date.now(),
          })),
          Effect.logWarning("effectmq Redis Sentinel topology changed", {
            role,
            eventType:
              typeof event === "object" && event !== null && "type" in event
                ? String(event.type)
                : "unknown",
          }),
          Metric.update(Observability.redisReconnects, 1),
        ]).pipe(Effect.asVoid),
      );
    });
  }

  const scope = yield* Effect.scope;
  yield* Scope.addFinalizer(
    scope,
    Effect.promise(() => client.close()),
  );

  const rawSend = <A = unknown>(
    command: string,
    args: ReadonlyArray<string | Uint8Array>,
    typeMapping?: typeof binaryTypeMapping,
  ): Promise<A> => {
    if (standaloneClient !== undefined) {
      return standaloneClient.sendCommand([command, ...args.map(toArg)], {
        typeMapping,
      }) as Promise<A>;
    }
    return sentinelClient?.sendCommand(false, [command, ...args.map(toArg)], {
      typeMapping,
    }) as Promise<A>;
  };

  const connect = yield* Effect.gen(function* () {
    yield* updateHealth(health, role, (current) => ({
      ...current,
      state: "connecting",
      lastChangeAt: Date.now(),
    }));
    yield* Effect.tryPromise({
      try: async () => await client.connect(),
      catch: (cause) => new Redis.RedisError({ cause }),
    });
    const clusterInfo = yield* Effect.tryPromise({
      try: () => rawSend<string>("INFO", ["cluster"]),
      catch: (cause) => new Redis.RedisError({ cause }),
    });
    if (/^cluster_enabled:1$/m.test(clusterInfo)) yield* unsupportedCluster();
    yield* updateHealth(health, role, (current) => ({
      ...current,
      state: "ready",
      lastChangeAt: Date.now(),
    }));
  }).pipe(Effect.cached);
  // Building the scoped Layer is the startup boundary: resolve topology and
  // establish every role before exposing command services.
  yield* connect;

  const sendWith =
    (typeMapping?: typeof binaryTypeMapping) =>
    <A = unknown>(
      command: string,
      ...args: ReadonlyArray<string | Uint8Array>
    ) =>
      Effect.gen(function* () {
        const result = yield* Effect.tryPromise({
          try: () => rawSend<A>(command, args, typeMapping),
          catch: (cause) => new Redis.RedisError({ cause }),
        }).pipe(
          Effect.tapError(() =>
            updateHealth(health, role, (current) => ({
              ...current,
              state: "degraded",
              commandErrors: current.commandErrors + 1,
              lastChangeAt: Date.now(),
            })),
          ),
        );
        yield* updateHealth(health, role, (current) => ({
          ...current,
          state: "ready",
          lastChangeAt:
            current.state === "ready" ? current.lastChangeAt : Date.now(),
        }));
        return result;
      });

  const send = sendWith(undefined);
  const sendBinary = sendWith(binaryTypeMapping);
  const redisPool = yield* makeRedisPool(send, sendBinary);
  return { redisPool, send } as const;
});

const make = Effect.fnUntraced(function* (config: RedisConfig = {}) {
  if (config.topology === "cluster") yield* unsupportedCluster();
  if ((config.topology ?? "standalone") === "standalone") {
    const invalid = validatePool((config as StandaloneRedisConfig).pool);
    if (invalid !== undefined) yield* invalid;
  }

  const topology: "standalone" | "sentinel" =
    config.topology === "sentinel" ? "sentinel" : "standalone";
  const health = yield* Ref.make<HealthState>({
    producer: initialRoleHealth(),
    worker: initialRoleHealth(),
    maintenance: initialRoleHealth(),
  });
  const supported = config as Exclude<RedisConfig, ClusterRedisConfig>;
  const producer = yield* makeClient(supported, "producer", health);
  const worker = yield* makeClient(supported, "worker", health);
  const maintenance = yield* makeClient(supported, "maintenance", health);
  const redis = yield* Redis.make({ send: producer.send });
  const snapshot = Ref.get(health).pipe(
    Effect.map(
      (current): RedisHealthSnapshot => ({
        topology,
        ready: roles.every((role) => current[role].state === "ready"),
        roles: current,
      }),
    ),
  );
  const connectionHealth = RedisConnectionHealth.of({
    snapshot,
    readiness: Effect.all([
      producer.redisPool.send("PING"),
      worker.redisPool.send("PING"),
      maintenance.redisPool.send("PING"),
    ]).pipe(
      Effect.as(true),
      Effect.catchCause(() => Effect.succeed(false)),
    ),
  });

  return Context.make(RedisPool, producer.redisPool).pipe(
    Context.add(
      RedisConnectionRoles,
      makeConnectionRoles(
        producer.redisPool,
        worker.redisPool,
        maintenance.redisPool,
      ),
    ),
    Context.add(RedisConnectionHealth, connectionHealth),
    Context.add(Redis.Redis, redis),
  );
});

export const layer = (
  config: RedisConfig = {},
): Layer.Layer<
  RedisPool | RedisConnectionRoles | RedisConnectionHealth | Redis.Redis,
  Redis.RedisError | UnsupportedRedisTopology | InvalidRedisConfiguration
> => Layer.effectContext(make(config));
