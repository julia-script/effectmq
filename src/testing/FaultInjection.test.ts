import { Effect } from "effect";
import { expect, layer } from "@effect/vitest";
import { RedisPool, TaskEngine } from "../index.js";
import * as FaultInjection from "./FaultInjection.js";
import { TestLayer } from "./redisLayer.js";

layer(TestLayer, { excludeTestServices: true, timeout: "60 seconds" })(
  "deterministic queue-boundary faults (real Redis time)",
  (it) => {
    it.layer(
      FaultInjection.layer({
        acknowledgement: 1,
        acquire: 1,
        cleanup: 1,
        event: 1,
        heartbeat: 1,
        offer: 1,
      }),
    )((it) => {
      it.effect(
        "retries safely around offer, acquire, heartbeat, ack, event, and cleanup",
        () =>
          Effect.gen(function* () {
            const engine = yield* TaskEngine.TaskEngine;
            const redis = yield* RedisPool.RedisPool;
            const fault = yield* FaultInjection.FaultInjection;
            const prefix = "fault-boundaries";
            yield* TaskEngine.setMockTime(5_000_000);
            const insert = {
              prefix,
              id: "task",
              name: "fault",
              payload: null,
              delay: 0,
              maxRetries: 0,
              onSuccessPolicy: "mark-as-success" as const,
              onFailurePolicy: "mark-as-failure" as const,
            };

            // The response is lost after Redis commits. Retrying the same identity
            // observes the original generation rather than creating another task.
            const offerFault = yield* fault
              .after("offer", engine.offerTask(insert))
              .pipe(Effect.flip);
            expect(offerFault).toMatchObject({
              _tag: "InjectedFault",
              point: "offer",
            });
            const duplicate = yield* engine.offerTask(insert);
            expect(duplicate.status).toBe("existing");
            expect(duplicate.task.generation).toBe(1);

            // Losing an acquire response leaves a fenced lease, not a second owner.
            const acquireFault = yield* fault
              .after("acquire", engine.takeTask(prefix, 100))
              .pipe(Effect.flip);
            expect(acquireFault).toMatchObject({ point: "acquire" });
            expect(
              yield* redis.send("ZCARD", `~effectmq:v1:${prefix}:active`),
            ).toBe(1);
            expect(yield* engine.takeTask(prefix, 100)).toBeNull();
            yield* redis.send("DEL", `~effectmq:v1:${prefix}:lock:task`);
            yield* TaskEngine.stepMockTime(101);
            yield* engine.maintain(prefix);
            const attempt = yield* engine.takeTask(prefix, 100);
            if (attempt === null) throw new Error("expected recovered attempt");

            const heartbeatFault = yield* fault
              .before(
                "heartbeat",
                engine.extendLock(prefix, "task", attempt.leaseToken, 10_000),
              )
              .pipe(Effect.flip);
            expect(heartbeatFault).toMatchObject({ point: "heartbeat" });
            yield* engine.extendLock(
              prefix,
              "task",
              attempt.leaseToken,
              10_000,
            );

            // Completion and its event are one atomic script. Losing either response
            // cannot undo the terminal state, and the old token is fenced.
            const ackFault = yield* fault
              .after(
                "acknowledgement",
                engine.writeSuccess(prefix, "task", attempt.leaseToken, "done"),
              )
              .pipe(Effect.flip);
            expect(ackFault).toMatchObject({ point: "acknowledgement" });
            const lateAck = yield* engine
              .writeSuccess(prefix, "task", attempt.leaseToken, "late")
              .pipe(Effect.flip);
            expect(lateAck).toMatchObject({ _tag: "LeaseLost" });

            const eventFault = yield* fault
              .after("event", engine.eventCursors(prefix))
              .pipe(Effect.flip);
            expect(eventFault).toMatchObject({ point: "event" });
            const cursors = yield* engine.eventCursors(prefix);
            expect(cursors.latest).not.toBe("0-0");

            const cleanupFault = yield* fault
              .before("cleanup", engine.maintain(prefix))
              .pipe(Effect.flip);
            expect(cleanupFault).toMatchObject({ point: "cleanup" });
            const health = yield* engine.maintain(prefix);
            expect(health.processed).toBeLessThanOrEqual(100);
          }),
      );
    });
  },
);
