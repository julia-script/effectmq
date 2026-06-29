import { Schema } from "effect";
import type { AnyStructSchema } from "effect/unstable/workflow/Workflow";
import type { TaskSchema } from "./Schemas.js";

const TypeId = "~effectmq/Task" as const;

// const makeTaskSchema = <
// 	Payload extends AnyStructSchema,
// 	Success extends Schema.Top,
// 	Error extends Schema.Top,
// >(
// 	payloadSchema: Payload,
// 	successSchema: Success,
// 	errorSchema: Error,
// ) =>
// 	Schema.Struct({
// 		...TaskEngine.EngineTaskSchema.fields,
// 		success: Schema.optional(successSchema),
// 		errors: Schema.Array(
// 			Schema.Union([TaskEngine.EngineErrorSchema, errorSchema]),
// 		),
// 		payload: Schema.fromJsonString(payloadSchema),
// 	});

// const abc: Schema.Codec<{ a: string; b: number }, { a: string; b: string }> =
// 	Schema.Struct({
// 		a: Schema.String,
// 		b: Schema.Number.pipe(Schema.fromJsonString),
// 	});

// type TaskSchema<
// 	Payload extends AnyStructSchema,
// 	Success extends Schema.Top,
// 	Error extends Schema.Top,
// > = Schema.Schema<Task<Payload["Type"], Success["Type"], Error["Type"]>>;

// const makeTaskFromEngineTask = <
// 	Payload extends AnyStructSchema,
// 	Success extends Schema.Top,
// 	Error extends Schema.Top,
// >(
// 	payloadSchema: Payload,
// 	successSchema: Success,
// 	errorSchema: Error,
// ): Schema.decodeTo<
// 	TaskSchema<Payload, Success, Error>,
// 	typeof TaskEngine.EngineTaskSchema
// > => {
// 	const a = payloadSchema.pipe(Schema.fromJsonString);
// 	// throw new Error("Not implemented");
// 	const b = Schema.decodeTo<TaskSchema<Payload, Success, Error>>(
// 		Schema.Struct({
// 			...TaskEngine.EngineTaskSchema.fields,
// 			payload: a,
// 			success: Schema.optional(successSchema),
// 			errors: Schema.Array(
// 				Schema.Union([TaskEngine.EngineErrorSchema, errorSchema]),
// 			),
// 		}),
// 		// Schema.Struct({
// 		// 	...TaskEngine.EngineTaskSchema.fields,
// 		// 	payload: payloadSchema.pipe(Schema.fromJsonString),
// 		// 	success: Schema.optional(successSchema.pipe(Schema.fromJsonString)),
// 		// 	errors: Schema.Array(
// 		// 		Schema.Union([
// 		// 			TaskEngine.EngineErrorSchema,
// 		// 			errorSchema.pipe(Schema.fromJsonString),
// 		// 		]),
// 		// 	),
// 		// }),
// 		// {
// 		//   decode: SchemaGetter.transform((value) => JSON.parse(value)),
// 		//   encode: SchemaGetter.transform((value) => JSON.stringify(value)),
// 		// }
// 		// ),
// 	);
// 	return b;
// 	// return Schema.Struct({
// 	// 	...TaskEngine.EngineTaskSchema.fields,
// 	// 	payload: payloadSchema.pipe(
// 	//     Schema.encodeTo(Schema.String, {
// 	//       decode: SchemaGetter.transform((value) => JSON.parse(value)),
// 	//       encode: SchemaGetter.transform((value) => JSON.stringify(value)),
// 	//     })
// 	//   ),

// 	// 	// success: Schema.optional(Schema.fromJsonString(successSchema)),
// 	// 	// errors: Schema.Array(
// 	// 	// 	Schema.Union([TaskEngine.EngineErrorSchema, errorSchema]),
// 	// 	// ),
// 	// });
// };

// const makeTaskSchema2 = <Payload extends AnyStructSchema, Success extends Schema.Top, Error extends Schema.Top>(config: {
//   payload: Payload,
//   success: Success,
//   error: Error,
// }): Schema.Schema<Task<Payload["Type"], Success["Type"], Error["Type"]>> => Schema.TaggedStruct("Task", {
//   id: Schema.String,
//   // ...TaskEngine.EngineTaskSchema.fields,
//   payload: Schema.toType(config.payload),
//   success: Schema.optional(Schema.toType(config.success)),
//   errors: Schema.Array(
//     Schema.Union([TaskEngine.EngineErrorSchema, Schema.toType(config.error)]),
//   ),
// })

// class Circle<T extends Schema.Top> extends Schema.TaggedClass<Circle>()("Circle", {
//   radius: Schema.Number

// }) {}

// class TaskSchema<Payload extends AnyStructSchema, Success extends Schema.Top, Error extends Schema.Top> extends Schema.TaggedClass<TaskSchema>()(
export type Task<
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
> = TaskSchema<Payload, Success, Error>["Type"] & { id: string; name: string };

export interface TaskDefinition<
  Payload extends AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
> {
  readonly [TypeId]: typeof TypeId;

  readonly name: string;
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly errorSchema: Error;

  readonly idempotencyKey: (payload: Payload["Type"]) => string;
  // readonly offer: (
  // 	payload: Payload["Type"],
  // 	options?: TaskOptions,
  // ) => Effect.Effect<
  // 	string,
  // 	SchemaError | TaskEngine.TaskEngineError,
  // 	TaskEngine.TaskEngine | Payload["EncodingServices"]
  // >;
}
type ResolvePayload<T extends AnyStructSchema | Schema.Struct.Fields> =
  T extends AnyStructSchema
    ? T
    : Schema.Struct<T extends Schema.Struct.Fields ? T : never>;

const resolvePayloadSchema = <T extends AnyStructSchema | Schema.Struct.Fields>(
  payload: T,
): ResolvePayload<T> => {
  return Schema.isSchema(payload)
    ? (payload as ResolvePayload<T>)
    : (Schema.Struct(payload) as ResolvePayload<T>);
};
export const make = <
  Payload extends AnyStructSchema | Schema.Struct.Fields,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(config: {
  name: string;
  successSchema: Success;
  errorSchema: Error;
  payload: Payload;
  idempotencyKey?: (payload: Payload["Type"]) => string;
}): TaskDefinition<ResolvePayload<Payload>, Success, Error> => {
  const payloadSchema = resolvePayloadSchema(config.payload);
  const successSchema = (config.successSchema ?? Schema.Void) as Success;
  const errorSchema = (config.errorSchema ?? Schema.Never) as Error;

  const self: TaskDefinition<ResolvePayload<Payload>, Success, Error> = {
    [TypeId]: TypeId,
    name: config.name,
    payloadSchema: payloadSchema,
    successSchema: successSchema,
    errorSchema: errorSchema,

    idempotencyKey:
      config.idempotencyKey ?? (() => `${config.name}/${crypto.randomUUID()}`),
  };

  return self;
};
