import { expect, it } from "vitest";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as RedisPool from "./RedisPool.js";
import * as TaskEngine from "./TaskEngine.js";
import * as TaskQueue from "./TaskQueue.js";
import * as TaskRecord from "./TaskRecord.js";
import type {
  Equal,
  Expect,
  ExpectFalse,
  IsAny,
  IsUnknown,
} from "./testing/TypeAssertions.js";

class QueueService extends Context.Service<QueueService, object>()(
  "@effectmq/core/test/QueueService",
) {}
class HandlerService extends Context.Service<HandlerService, object>()(
  "@effectmq/core/test/HandlerService",
) {}
class IdentityService extends Context.Service<IdentityService, object>()(
  "@effectmq/core/test/IdentityService",
) {}

const Payload = Schema.String;
const Success = Schema.Number;
const Failure = Schema.Boolean;

const compilePublicContracts = () => {
  const queue = undefined as unknown as TaskQueue.TaskQueue<
    typeof Payload,
    typeof Success,
    typeof Failure,
    QueueService,
    IdentityService
  >;
  const handler = undefined as unknown as TaskQueue.TaskHandler<
    typeof Payload,
    typeof Success,
    typeof Failure,
    HandlerService
  >;
  const handle = undefined as unknown as TaskQueue.TaskHandle<number, boolean>;
  const engineTask = undefined as unknown as Parameters<
    typeof TaskRecord.decodeTask
  >[1];

  const complete = TaskQueue.complete(queue, handler);
  type CompleteSuccess = Expect<Equal<Effect.Success<typeof complete>, string>>;
  type CompleteError = Expect<
    Equal<Effect.Error<typeof complete>, TaskQueue.CompleteError>
  >;
  type CompleteServices = Expect<
    Equal<
      Effect.Services<typeof complete>,
      TaskQueue.CompleteRequirements<
        typeof Payload,
        typeof Success,
        typeof Failure,
        QueueService,
        HandlerService
      >
    >
  >;

  const completeOne = TaskQueue.completeOne(queue, handler);
  type CompleteOneSuccess = Expect<
    Equal<Effect.Success<typeof completeOne>, boolean>
  >;
  type CompleteOneError = Expect<
    Equal<Effect.Error<typeof completeOne>, TaskQueue.CompleteError>
  >;
  type CompleteOneServices = Expect<
    Equal<Effect.Services<typeof completeOne>, Effect.Services<typeof complete>>
  >;

  const decoded = TaskRecord.decodeTask(
    {
      schemaId: "contract",
      payloadSchema: Payload,
      successSchema: Success,
      errorSchema: Failure,
    },
    engineTask,
  );
  type DecodeSuccess = Expect<
    Equal<
      Effect.Success<typeof decoded>,
      TaskRecord.Task<typeof Payload, typeof Success, typeof Failure>
    >
  >;
  type DecodeServices = Expect<Equal<Effect.Services<typeof decoded>, never>>;

  const waited = TaskQueue.wait(queue, handle);
  type WaitSuccess = Expect<Equal<Effect.Success<typeof waited>, number>>;
  type WaitError = Expect<
    Equal<Effect.Error<typeof waited>, TaskQueue.WaitError<boolean>>
  >;
  type WaitServices = Expect<
    Equal<
      Effect.Services<typeof waited>,
      TaskQueue.WaitRequirements<typeof Payload, typeof Success, typeof Failure>
    >
  >;

  const executed = TaskQueue.execute(queue, "payload");
  type ExecuteSuccess = Expect<Equal<Effect.Success<typeof executed>, number>>;
  type ExecuteError = Expect<
    Equal<Effect.Error<typeof executed>, TaskQueue.ExecuteError<boolean>>
  >;
  type ExecuteServices = Expect<
    Equal<
      Effect.Services<typeof executed>,
      TaskQueue.ExecuteRequirements<
        typeof Payload,
        typeof Success,
        typeof Failure,
        IdentityService
      >
    >
  >;

  type RejectErasedError = ExpectFalse<
    Equal<Effect.Error<typeof executed>, never>
  >;
  type RejectAnyError = ExpectFalse<IsAny<Effect.Error<typeof executed>>>;
  type RejectUnknownError = ExpectFalse<
    IsUnknown<Effect.Error<typeof executed>>
  >;
  type RejectErasedServices = ExpectFalse<
    Equal<Effect.Services<typeof complete>, never>
  >;
  type RejectAnyServices = ExpectFalse<IsAny<Effect.Services<typeof complete>>>;

  const layerNoDeps = TaskEngine.layerNoDeps();
  const liveLayer = TaskEngine.layer();
  type LayerNoDepsRequirement = Expect<
    Equal<Layer.Services<typeof layerNoDeps>, RedisPool.RedisPool>
  >;
  type LiveLayerRequirement = Expect<
    Equal<Layer.Services<typeof liveLayer>, never>
  >;

  return undefined as unknown as
    | CompleteSuccess
    | CompleteError
    | CompleteServices
    | CompleteOneSuccess
    | CompleteOneError
    | CompleteOneServices
    | DecodeSuccess
    | DecodeServices
    | WaitSuccess
    | WaitError
    | WaitServices
    | ExecuteSuccess
    | ExecuteError
    | ExecuteServices
    | RejectErasedError
    | RejectAnyError
    | RejectUnknownError
    | RejectErasedServices
    | RejectAnyServices
    | LayerNoDepsRequirement
    | LiveLayerRequirement;
};

it("pins public Effect and Layer channels at compile time", () => {
  expect(typeof compilePublicContracts).toBe("function");
});
