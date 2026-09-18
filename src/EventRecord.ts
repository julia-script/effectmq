/** Public records for durable application events and their recipients. @module */
import * as Schema from "effect/Schema";

/** A recipient's progress, independent of the other subscriptions. */
export const RecipientSchema = Schema.Struct({
  name: Schema.String,
  generation: Schema.String,
  status: Schema.Literals(["pending", "acknowledged", "waived"]),
  leaseToken: Schema.NullOr(Schema.String),
  leaseUntil: Schema.NullOr(Schema.Number),
});
export type Recipient = typeof RecipientSchema.Type;

/** Versioned storage record. Payload remains an opaque storage envelope. */
export const EncodedEventSchema = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  queue: Schema.String,
  payload: Schema.String,
  createdAt: Schema.Number,
  resolvedAt: Schema.Number,
  expiresAt: Schema.NullOr(Schema.Number),
  settledAt: Schema.NullOr(Schema.Number),
  archiveUntil: Schema.NullOr(Schema.Number),
  status: Schema.Literals(["active", "completed", "expired"]),
  recipients: Schema.Record(Schema.String, RecipientSchema),
});
export type EncodedEvent = typeof EncodedEventSchema.Type;

/** A schema-decoded event; timestamps are Unix milliseconds. */
export interface Event<Payload> extends Omit<EncodedEvent, "payload"> {
  readonly payload: Payload;
}

/** Durable identity returned by registration. Keep the generation unchanged. */
export interface Subscription {
  readonly queue: string;
  readonly name: string;
  readonly generation: string;
}

/** One fenced delivery attempt. Pass this handle unchanged to acknowledgement. */
export interface Delivery<Payload> {
  readonly event: Event<Payload>;
  readonly subscription: Subscription;
  readonly leaseToken: string;
}
