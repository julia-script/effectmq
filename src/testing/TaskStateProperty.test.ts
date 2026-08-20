import { Effect } from "effect";
import { expect, layer } from "@effect/vitest";
import { RedisPool, TaskEngine } from "../index.js";
import { TestLayer } from "./redisLayer.js";
import * as Model from "./TaskStateModel.js";

const value = <A>(result: Model.ModelResult<A>): A => {
  if (!result.ok) throw new Error(result.error);
  return result.value;
};

const randomFor = (seed: number) => {
  let state = seed >>> 0;
  return () => {
    state += 0x6d2b79f5;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
};

const expectedList = (state: Model.ExecutionState) => {
  switch (state) {
    case "waiting":
    case "retry-scheduled":
      return "wait";
    case "leased":
      return "active";
    case "delayed":
      return "scheduled";
    case "succeeded":
      return "success";
    case "failed":
      return "failed";
  }
};

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "generated Redis state-machine sequences (real Redis time)",
  (it) => {
    it.effect(
      "preserve ownership, terminal, relationship, and work-bound invariants",
      () =>
        Effect.gen(function* () {
          const redis = yield* RedisPool.RedisPool;
          const engine = yield* TaskEngine.TaskEngine;
          const now = 2_000_000_000_000;
          // Redis lock expiry uses real wall-clock TTLs even when the engine's
          // due-time clock is mocked. Keep ordinary generated attempts well
          // clear of incidental expiry; expiry branches advance mock time and
          // remove the lock explicitly below.
          const leaseMs = 30_000;
          yield* TaskEngine.setMockTime(now);

          for (let seed = 1; seed <= 32; seed++) {
            const random = randomFor(seed);
            const prefix = `property-${seed}`;
            const internal = `~effectmq:v1:${prefix}`;
            const id = "task";
            let model = value(Model.offer(Model.make(), { id }));
            let renewals = 0;
            let operations = 0;
            let terminal = false;
            yield* engine.createTask({
              prefix,
              id,
              name: "property",
              payload: { seed },
              delay: 0,
              maxRetries: 100,
              maxStalledCount: 2,
              onSuccessPolicy: "mark-as-success",
              onFailurePolicy: "mark-as-failure",
            });

            const assertRedisInvariants = Effect.fnUntraced(function* () {
              Model.assertInvariants(model);
              const task = model.tasks.get(id);
              const lists = {
                wait: yield* redis.send<ReadonlyArray<string>>(
                  "LRANGE",
                  `${internal}:wait`,
                  "0",
                  "-1",
                ),
                scheduled: yield* redis.send<ReadonlyArray<string>>(
                  "ZRANGE",
                  `${internal}:scheduled`,
                  "0",
                  "-1",
                ),
                active: yield* redis.send<ReadonlyArray<string>>(
                  "ZRANGE",
                  `${internal}:active`,
                  "0",
                  "-1",
                ),
                success: yield* redis.send<ReadonlyArray<string>>(
                  "ZRANGE",
                  `${internal}:success`,
                  "0",
                  "-1",
                ),
                failed: yield* redis.send<ReadonlyArray<string>>(
                  "ZRANGE",
                  `${internal}:failed`,
                  "0",
                  "-1",
                ),
              };
              const memberships = Object.entries(lists)
                .filter(([, ids]) => ids.includes(id))
                .map(([name]) => name);
              if (task === undefined) {
                expect(
                  memberships,
                  `seed ${seed}, operation ${operations}`,
                ).toEqual([]);
                expect(yield* engine.getTask(prefix, id)).toBeNull();
                return;
              }
              expect(
                memberships,
                `seed ${seed}, operation ${operations}`,
              ).toEqual([expectedList(task.state)]);
              const lockExists = Number(
                yield* redis.send("EXISTS", `${internal}:lock:${id}`),
              );
              expect(lockExists).toBe(task.state === "leased" ? 1 : 0);
              expect(
                Number(
                  yield* redis.send(
                    "SCARD",
                    `${internal}:task:${id}:${task.generation}:retained-by`,
                  ),
                ),
              ).toBe(task.retainedBy.size);
              const stored = yield* engine.getTask(prefix, id);
              expect(stored?.handlerFailureCount).toBe(
                task.handlerFailureCount,
              );
              expect(stored?.stalledAttemptCount).toBe(
                task.stalledAttemptCount,
              );
              const health = yield* engine.maintain(prefix);
              expect(health.processed).toBeLessThanOrEqual(100);
            });

            yield* assertRedisInvariants();
            while (!terminal && operations < 20) {
              operations++;
              const task = model.tasks.get(id);
              if (task === undefined) break;
              if (
                task.state === "waiting" ||
                task.state === "retry-scheduled"
              ) {
                const attempt = yield* engine.takeTask(prefix, leaseMs);
                expect(attempt).not.toBeNull();
                if (attempt === null)
                  throw new Error("expected generated attempt");
                model = value(Model.acquire(model, id, attempt.leaseToken));
              } else if (task.state === "leased") {
                const choice = random();
                if (choice < 0.18 && renewals < 2) {
                  yield* engine.extendLock(
                    prefix,
                    id,
                    task.leaseToken ?? "",
                    leaseMs,
                  );
                  model = value(Model.renew(model, id, task.leaseToken ?? ""));
                  renewals++;
                } else if (choice < 0.36) {
                  yield* engine.removeLock(prefix, id, task.leaseToken ?? "");
                  model = value(
                    Model.fail(model, {
                      id,
                      leaseToken: task.leaseToken ?? "",
                      retry: true,
                    }),
                  );
                  // Voluntary release and a retry are both immediately runnable;
                  // correct the model's failure count because release is not a failure.
                  const released = model.tasks.get(id);
                  if (released !== undefined) {
                    model = {
                      tasks: new Map(model.tasks).set(id, {
                        ...released,
                        handlerFailureCount: released.handlerFailureCount - 1,
                      }),
                    };
                  }
                } else if (choice < 0.58) {
                  yield* engine.writeError(
                    prefix,
                    id,
                    task.leaseToken ?? "",
                    { reason: "retry", seed },
                    now,
                  );
                  model = value(
                    Model.fail(model, {
                      id,
                      leaseToken: task.leaseToken ?? "",
                      retry: true,
                    }),
                  );
                } else if (choice < 0.76) {
                  yield* redis.send("DEL", `${internal}:lock:${id}`);
                  yield* TaskEngine.stepMockTime(leaseMs + 1);
                  yield* engine.maintain(prefix);
                  model = value(
                    Model.expire(model, { id, maxStalledCount: 2 }),
                  );
                } else if (choice < 0.9) {
                  yield* engine.writeSuccess(
                    prefix,
                    id,
                    task.leaseToken ?? "",
                    {
                      ok: true,
                    },
                  );
                  model = value(
                    Model.succeed(model, id, task.leaseToken ?? ""),
                  );
                  terminal = true;
                } else {
                  yield* engine.writeError(prefix, id, task.leaseToken ?? "", {
                    reason: "terminal",
                    seed,
                  });
                  model = value(
                    Model.fail(model, {
                      id,
                      leaseToken: task.leaseToken ?? "",
                      retry: false,
                    }),
                  );
                  terminal = true;
                }
              } else {
                terminal = true;
              }
              yield* assertRedisInvariants();
            }

            const final = model.tasks.get(id);
            if (
              final?.state === "waiting" ||
              final?.state === "retry-scheduled"
            ) {
              const attempt = yield* engine.takeTask(prefix, leaseMs);
              if (attempt === null) throw new Error("expected final attempt");
              model = value(Model.acquire(model, id, attempt.leaseToken));
            }
            const leased = model.tasks.get(id);
            if (leased?.state === "leased") {
              yield* engine.writeSuccess(
                prefix,
                id,
                leased.leaseToken ?? "",
                "forced-terminal",
              );
              model = value(Model.succeed(model, id, leased.leaseToken ?? ""));
              operations++;
              yield* assertRedisInvariants();
            }
            yield* engine.removeTask(prefix, id);
            model = value(Model.remove(model, id));
            operations++;
            yield* assertRedisInvariants();
          }
        }),
      30_000,
    );
  },
);
