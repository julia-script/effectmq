import { Fiber, Schedule } from "effect";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Function from "effect/Function";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import { type CompletionPolicy, makeTaskSchema } from "./Schemas.js";
import type * as Task from "./Task.js";
import * as TaskEngine from "./TaskEngine.js";

const TypeId = "~effectmq/TaskQueue" as const;

export interface TaskQueue<
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
> {
  readonly [TypeId]: typeof TypeId;
  readonly name: string;
  readonly task: Task.TaskDefinition<Payload, Success, Error>;
}
export const make = <
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  name: string,
  taskDefinition: Task.TaskDefinition<Payload, Success, Error>,
): TaskQueue<Payload, Success, Error> => {
  return {
    [TypeId]: TypeId,
    name,
    task: taskDefinition,
  };
};

interface TakeOptions {
  readonly lockTimeout?: Duration.Input;
  readonly poolInterval?: Duration.Input;
}
export const takeUnsafe = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  queue: TaskQueue<Payload, Success, Error>,
  options?: TakeOptions,
): Effect.fn.Return<
  Task.Task<Payload, Success, Error>,
  TaskEngine.TaskEngineError | Schema.SchemaError,
  TaskEngine.TaskEngine | Payload["DecodingServices"]
> {
  const engine = yield* TaskEngine.TaskEngine;
  const poolInterval = Duration.toMillis(
    options?.poolInterval ?? Duration.seconds(1),
  );
  const lockTimeout = Duration.toMillis(
    options?.lockTimeout ?? Duration.seconds(30),
  );

  while (true) {
    const task = yield* engine.takeTask(
      TaskEngine.makePrefix(queue.name),
      lockTimeout,
    );

    if (task) {
      const taskSchema = makeTaskSchema({
        payload: queue.task.payloadSchema,
        success: queue.task.successSchema,
        error: queue.task.errorSchema,
      });
      const decode = Schema.decodeEffect(taskSchema);
      const decodedTask = yield* decode(task);
      return decodedTask as Task.Task<Payload, Success, Error>;
    }
    yield* Effect.sleep(poolInterval);
  }
});

const encodeToString = <A extends Schema.Top>(schema: A) =>
  Schema.encodeEffect(Schema.fromJsonString(schema));
export interface TaskOptions {
  delay?: number;
  maxRetries?: number;
  onSuccessPolicy?: CompletionPolicy;
  onFailurePolicy?: CompletionPolicy;
}
export const offer = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  payload: Payload["Type"],
  options?: TaskOptions,
) {
  const encodePayload = encodeToString(queue.task.payloadSchema);
  const id = queue.task.idempotencyKey(payload);
  const engine = yield* TaskEngine.TaskEngine;

  const task = yield* engine.createTask({
    prefix: queue.name,
    id,
    name: queue.task.name,
    payload: yield* encodePayload(payload),
    delay: options?.delay ?? 0,
    maxRetries: options?.maxRetries ?? 0,
    onSuccessPolicy: options?.onSuccessPolicy ?? "delete",
    onFailurePolicy: options?.onFailurePolicy ?? "delete",
  });
  return task;
});

export const extendLock = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  lockTimeout?: Duration.Input,
) {
  const engine = yield* TaskEngine.TaskEngine;

  yield* Effect.log(
    `extending lock for task ${task.id} with timeout ${lockTimeout}`,
  );
  return yield* engine.extendLock(
    queue.name,
    task.id,
    Duration.toMillis(lockTimeout ?? Duration.seconds(30)),
  );
});

export const release = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(queue: TaskQueue<Payload, Success, Error>, taskId: string) {
  const engine = yield* TaskEngine.TaskEngine;
  return yield* engine.removeLock(queue.name, taskId);
});

const succeed = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  success: Success["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  const encode = encodeToString(queue.task.successSchema);
  return yield* engine.writeSuccess(
    queue.name,
    task.id,
    yield* encode(success),
  );
});

export const fail = Effect.fnUntraced(function* <
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  queue: TaskQueue<Payload, Success, Error>,
  task: Task.Task<Payload, Success, Error>,
  failure: Error["Type"],
) {
  const engine = yield* TaskEngine.TaskEngine;
  const encode = encodeToString(queue.task.errorSchema);
  return yield* engine.writeError(queue.name, task.id, yield* encode(failure));
});

export type TaskHandler<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R,
> = (
  task: Task.Task<Payload, Success, Error>,
) => Effect.Effect<Success["Type"], Error["Type"], R>;

export const complete: {
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    handler: TaskHandler<Payload, Success, Error, R>,
  ): (
    self: TaskQueue<Payload, Success, Error>,
  ) => Effect.Effect<boolean, never, R>;
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    self: TaskQueue<Payload, Success, Error>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ): Effect.Effect<boolean, never, R>;
} = Function.dual(
  2,
  <
    Payload extends AnyStructSchema,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R,
  >(
    self: TaskQueue<Payload, Success, Error>,
    handler: TaskHandler<Payload, Success, Error, R>,
  ) => {
    return Effect.gen(function* () {
      const task = yield* takeUnsafe(self);
      const lockTimeout = Duration.seconds(30);
      const lockRefresh = Duration.seconds(10);
      const policy = Schedule.spaced(lockRefresh);
      const heartBeat = yield* extendLock(self, task, lockTimeout)
        .pipe(Effect.repeat(policy))
        .pipe(Effect.forkChild);

      const result = yield* handler(task).pipe(Effect.result);
      yield* Fiber.interrupt(heartBeat);
      if (Result.isSuccess(result)) {
        yield* succeed(self, task, result.success);
        return true;
      } else {
        yield* fail(self, task, result.failure);
        return false;
      }
    });
  },
);
