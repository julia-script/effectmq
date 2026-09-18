/** Schema-typed durable events, named subscriptions, and managed delivery. @module */
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as EventEngine from "./EventEngine.js";
import type * as EventRecord from "./EventRecord.js";
import * as StorageProtocol from "./StorageProtocol.js";

/** A typed queue definition. Use the same schema and policy on every client. */
export interface EventQueue<Payload extends Schema.Top>
  extends EventEngine.Queue {
  readonly payload: Payload;
}
/** Errors from storage, ownership, and payload codecs. */
export type Error =
  | EventEngine.EventEngineError
  | StorageProtocol.StorageProtocolError
  | Schema.SchemaError;

/**
 * Define an event queue. Registration/emission persist and validate its policy.
 * Events and archives have no deadline by default; completed events are deleted.
 */
export const make = <Payload extends Schema.Top>(
  name: string,
  payload: Payload,
  options: Omit<EventEngine.Queue, "name"> = {},
): EventQueue<Payload> => ({ ...options, name, payload });

/** Idempotently register a durable recipient before emitting events for it. */
export const subscribe = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  name: string,
): Effect.fn.Return<
  EventRecord.Subscription,
  EventEngine.EventEngineError,
  EventEngine.EventEngine | Crypto.Crypto
> {
  return yield* (yield* EventEngine.EventEngine).subscribe(queue, name);
});

/** Remove this generation and waive its unfinished obligations; disconnecting does not remove it. */
export const unsubscribe = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  subscription: EventRecord.Subscription,
): Effect.fn.Return<
  boolean,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  return yield* (yield* EventEngine.EventEngine).unsubscribe(
    queue,
    subscription,
  );
});

/**
 * Emit to the current recipient set. A null ttlMs overrides a queue deadline with
 * indefinite waiting. Each call creates a distinct event, including retries.
 */
export const emit = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  payload: P["Type"],
  options: { readonly ttlMs?: number | null } = {},
): Effect.fn.Return<
  EventRecord.Event<P["Type"]>,
  Error,
  EventEngine.EventEngine | Crypto.Crypto | P["EncodingServices"]
> {
  const encoded = yield* Schema.encodeEffect(queue.payload)(payload);
  const envelope = yield* StorageProtocol.encodeValue(
    `event:${queue.name}`,
    "payload",
    encoded,
  );
  const event = yield* (yield* EventEngine.EventEngine).emit(
    queue,
    envelope,
    options.ttlMs,
  );
  return { ...event, payload };
});

const decode = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  event: EventRecord.EncodedEvent,
): Effect.fn.Return<
  EventRecord.Event<P["Type"]>,
  Error,
  P["DecodingServices"]
> {
  if (event.queue !== queue.name)
    return yield* new EventEngine.EventEngineError({
      code: "CorruptStorage",
      message: "Event belongs to a different queue",
    });
  const encoded = yield* StorageProtocol.decodeValue(
    event.payload,
    `event:${queue.name}`,
    "payload",
  );
  const payload = yield* Schema.decodeUnknownEffect(queue.payload)(encoded);
  return { ...event, payload };
});

/** Inspect an active or archived event. Deleted or retention-expired records return null. */
export const get = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  id: string,
): Effect.fn.Return<
  EventRecord.Event<P["Type"]> | null,
  Error,
  EventEngine.EventEngine | P["DecodingServices"]
> {
  const event = yield* (yield* EventEngine.EventEngine).get(queue, id);
  return event === null ? null : yield* decode(queue, event);
});

/**
 * Acquire one currently available delivery (or null), with a 30-second lease by
 * default. Expired leases are recoverable. This call does not block or poll.
 */
export const take = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  subscription: EventRecord.Subscription,
  options: { readonly leaseMs?: number } = {},
): Effect.fn.Return<
  EventRecord.Delivery<P["Type"]> | null,
  Error,
  EventEngine.EventEngine | Crypto.Crypto | P["DecodingServices"]
> {
  const attempt = yield* (yield* EventEngine.EventEngine).take(
    queue,
    subscription,
    options.leaseMs,
  );
  if (attempt === null) return null;
  return {
    event: yield* decode(queue, attempt.event),
    subscription,
    leaseToken: attempt.leaseToken,
  };
});

const identity = <P>(
  delivery: EventRecord.Delivery<P>,
): EventEngine.Attempt => ({
  ...delivery.subscription,
  id: delivery.event.id,
  token: delivery.leaseToken,
});

/** Resolve only this subscription's obligation. Duplicate calls cannot count twice. */
export const acknowledge = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  delivery: EventRecord.Delivery<P["Type"]>,
): Effect.fn.Return<
  EventEngine.Acknowledgement,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  return yield* (yield* EventEngine.EventEngine).acknowledge(
    queue,
    identity(delivery),
  );
});

/** Extend current delivery ownership. Stale tokens fail with LeaseLost. */
export const renew = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  delivery: EventRecord.Delivery<P["Type"]>,
  leaseMs = 30_000,
): Effect.fn.Return<
  void,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  yield* (yield* EventEngine.EventEngine).renew(
    queue,
    identity(delivery),
    leaseMs,
  );
});

/** Return a delivery for retry without resolving its acknowledgement obligation. */
export const release = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  delivery: EventRecord.Delivery<P["Type"]>,
  delayMs = 0,
): Effect.fn.Return<
  void,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  yield* (yield* EventEngine.EventEngine).release(
    queue,
    identity(delivery),
    delayMs,
  );
});

/** Apply a bounded pass of removals, event expiration, and archive cleanup. */
export const maintain = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
): Effect.fn.Return<
  EventEngine.MaintenanceResult,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  return yield* (yield* EventEngine.EventEngine).maintain(queue);
});

/** List retained archive ids in settlement order. Offsets are not snapshot cursors. */
export const listArchived = Effect.fnUntraced(function* <P extends Schema.Top>(
  queue: EventQueue<P>,
  options?: { readonly offset?: number; readonly limit?: number },
): Effect.fn.Return<
  ReadonlyArray<string>,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  return yield* (yield* EventEngine.EventEngine).listArchived(queue, options);
});

/** Run alongside consumers for timely cleanup even when the queue is idle. */
export const runMaintenance = Effect.fnUntraced(function* <
  P extends Schema.Top,
>(
  queue: EventQueue<P>,
  intervalMs = 1_000,
): Effect.fn.Return<
  never,
  EventEngine.EventEngineError,
  EventEngine.EventEngine
> {
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 1)
    return yield* new EventEngine.EventEngineError({
      code: "InvalidInput",
      message: "intervalMs must be a positive integer",
    });
  while (true) {
    const result = yield* maintain(queue);
    yield* Effect.sleep(result.pending ? 1 : intervalMs);
  }
});

/** Controls one managed delivery attempt; all durations are milliseconds. */
export interface ProcessingOptions {
  readonly leaseMs?: number;
  readonly renewEveryMs?: number;
  readonly retryDelayMs?: number;
}

/**
 * Process at most one event, renewing its lease and acknowledging only on success.
 * Returns false when nothing is available. Typed handler failures release the
 * delivery with a retry delay and propagate; crashes/interruption recover after
 * lease expiry. Repeat this effect to consume continuously. Make side effects
 * idempotent: an interrupted attempt can have performed them before acknowledgement.
 */
export const processOne = Effect.fnUntraced(function* <
  P extends Schema.Top,
  E,
  R,
>(
  queue: EventQueue<P>,
  subscription: EventRecord.Subscription,
  handler: (
    event: EventRecord.Event<P["Type"]>,
  ) => Effect.Effect<unknown, E, R>,
  options: ProcessingOptions = {},
): Effect.fn.Return<
  boolean,
  Error | E,
  EventEngine.EventEngine | Crypto.Crypto | P["DecodingServices"] | R
> {
  const leaseMs = options.leaseMs ?? 30_000;
  const renewEveryMs =
    options.renewEveryMs ?? Math.max(1, Math.floor(leaseMs / 3));
  const retryDelayMs = options.retryDelayMs ?? 1_000;
  if (
    !Number.isSafeInteger(leaseMs) ||
    leaseMs < 2 ||
    !Number.isSafeInteger(renewEveryMs) ||
    renewEveryMs < 1 ||
    renewEveryMs >= leaseMs ||
    !Number.isSafeInteger(retryDelayMs) ||
    retryDelayMs < 0 ||
    retryDelayMs > 3_153_600_000_000
  ) {
    return yield* new EventEngine.EventEngineError({
      code: "InvalidInput",
      message:
        "Require 0 < renewEveryMs < leaseMs and a nonnegative retryDelayMs",
    });
  }
  const delivery = yield* take(queue, subscription, { leaseMs });
  if (delivery === null) return false;
  const heartbeat = renew(queue, delivery, leaseMs).pipe(
    Effect.repeat(Schedule.spaced(renewEveryMs)),
    Effect.flatMap(() => Effect.never),
  );
  const result = yield* Effect.raceFirst(
    Effect.suspend(() => handler(delivery.event)).pipe(Effect.result),
    heartbeat,
  );
  if (Result.isFailure(result)) {
    yield* release(queue, delivery, retryDelayMs);
    return yield* Effect.fail(result.failure);
  }
  const acknowledgement = yield* acknowledge(queue, delivery);
  if (acknowledgement === "gone")
    return yield* new EventEngine.EventEngineError({
      code: "LeaseLost",
      message: "Event was removed before managed acknowledgement",
    });
  return true;
});
