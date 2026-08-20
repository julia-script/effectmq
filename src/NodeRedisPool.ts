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

/**
 * node-redis connection options accepted by a standalone EffectMQ pool.
 *
 * Both RESP2 and RESP3 are covered by the compatibility suite.
 *
 * @category Configuration
 * @since 0.2.0
 */
export type RedisPoolOptions = Omit<
  RedisClientOptions,
  "clientSideCache" | "RESP"
> & {
  readonly RESP?: 2 | 3;
};
/**
 * The workload assigned to one isolated Redis connection service.
 *
 * @category Models
 * @since 0.3.0
 */
export type RedisRole = "producer" | "worker" | "maintenance";

/**
 * Bounds a standalone node-redis connection pool.
 *
 * Defaults are one minimum connection, 100 maximum connections, and 3-second
 * acquire and cleanup delays. Invalid or unbounded settings fail when the
 * layer is built.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface BoundedPoolOptions {
  readonly minimum?: number;
  readonly maximum?: number;
  readonly acquireTimeout?: number;
  readonly cleanupDelay?: number;
}

/**
 * Configures standalone Redis while retaining node-redis option compatibility.
 *
 * @category Configuration
 * @since 0.3.0
 */
export type StandaloneRedisConfig = RedisPoolOptions & {
  readonly topology?: "standalone";
  readonly pool?: BoundedPoolOptions;
};

/**
 * Configures Sentinel discovery of the current writable Redis primary.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface SentinelRedisConfig {
  readonly topology: "sentinel";
  readonly sentinel: RedisSentinelOptions;
}

/**
 * Represents Redis Cluster so it can be rejected as a typed configuration error.
 *
 * EffectMQ scripts operate atomically across multiple keys, which Redis
 * Cluster cannot guarantee unless every key shares a hash slot.
 *
 * @category Configuration
 * @since 0.3.0
 */
export interface ClusterRedisConfig {
  readonly topology: "cluster";
}

/**
 * Redis topology configuration accepted by {@link layer}.
 *
 * @category Configuration
 * @since 0.3.0
 */
export type RedisConfig =
  | StandaloneRedisConfig
  | SentinelRedisConfig
  | ClusterRedisConfig;

/**
 * Indicates that Redis Cluster was configured or detected.
 *
 * @category Errors
 * @since 0.3.0
 */
export class UnsupportedRedisTopology extends Data.TaggedError(
  "UnsupportedRedisTopology",
)<{
  readonly topology: "cluster";
  readonly reason: string;
}> {}

/**
 * Indicates that bounded pool settings are inconsistent or outside safe limits.
 *
 * @category Errors
 * @since 0.3.0
 */
export class InvalidRedisConfiguration extends Data.TaggedError(
  "InvalidRedisConfiguration",
)<{ readonly reason: string }> {}

/**
 * Passive connection and command health recorded for one Redis role.
 *
 * @category Models
 * @since 0.3.0
 */
export interface RedisRoleHealth {
  readonly state: "disconnected" | "connecting" | "ready" | "degraded";
  readonly commandErrors: number;
  readonly reconnects: number;
  readonly lastChangeAt: number;
}

/**
 * A secret-free snapshot of all EffectMQ Redis connection roles.
 *
 * @category Models
 * @since 0.3.0
 */
export interface RedisHealthSnapshot {
  readonly topology: "standalone" | "sentinel";
  readonly ready: boolean;
  readonly roles: Readonly<Record<RedisRole, RedisRoleHealth>>;
}

/**
 * Exposes passive connection state and an active readiness probe.
 *
 * `snapshot` reads locally recorded state. `readiness` sends `PING` through all
 * three role services and succeeds with `false` when any probe fails.
 *
 * @category Services
 * @since 0.3.0
 */
export interface RedisConnectionHealthService {
  readonly snapshot: Effect.Effect<RedisHealthSnapshot>;
  readonly readiness: Effect.Effect<boolean>;
}

/**
 * Effect service tag for Redis connection health and readiness.
 *
 * @category Services
 * @since 0.3.0
 */
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

/**
 * Creates scoped Redis services for EffectMQ.
 *
 * The layer establishes independent producer, worker, and maintenance
 * connections before exposing any service. All connections close with the
 * layer scope. Standalone Redis and Sentinel are supported; Cluster fails with
 * {@link UnsupportedRedisTopology}.
 *
 * **Example: Provide a standalone Redis connection**
 *
 * ```ts
 * import { Effect } from "effect"
 * import { NodeRedisPool, RedisPool } from "@effectmq/core"
 *
 * const program = Effect.gen(function* () {
 *   const redis = yield* RedisPool.RedisPool
 *   return yield* redis.send<string>("PING")
 * }).pipe(
 *   Effect.provide(NodeRedisPool.layer({ url: "redis://127.0.0.1:6379" }))
 * )
 * ```
 *
 * @category Layers
 * @since 0.2.0
 */
export const layer = (
  config: RedisConfig = {},
): Layer.Layer<
  RedisPool | RedisConnectionRoles | RedisConnectionHealth | Redis.Redis,
  Redis.RedisError | UnsupportedRedisTopology | InvalidRedisConfiguration
> => Layer.effectContext(make(config));
