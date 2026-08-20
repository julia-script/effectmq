/** Runtime invariant checks for pure task definitions. @internal */
import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import type * as Task from "./Task.js";

const invalid = (
  taskName: string,
  field: string,
  constraint: string,
  actual: unknown,
) =>
  Effect.die(
    new Error(
      `Invalid task definition "${taskName}": ${field} must be ${constraint}; received ${String(actual)}`,
    ),
  );

/** Validates programmer-authored task configuration at its first runtime use. */
export const validate = Effect.fnUntraced(function* <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R,
  IdentityR,
>(definition: Task.TaskDefinition<Payload, Success, Error, R, IdentityR>) {
  if (
    definition.maxRetries !== Infinity &&
    (!Number.isSafeInteger(definition.maxRetries) || definition.maxRetries < 0)
  ) {
    return yield* invalid(
      definition.name,
      "maxRetries",
      "a non-negative safe integer or Infinity",
      definition.maxRetries,
    );
  }

  for (const [field, actual] of Object.entries(definition.storageLimits)) {
    const minimum = field === "maxEventEntries" ? 1 : 0;
    if (!Number.isSafeInteger(actual) || actual < minimum) {
      return yield* invalid(
        definition.name,
        field,
        `a safe integer greater than or equal to ${minimum}`,
        actual,
      );
    }
  }

  for (const [field, actual] of Object.entries(definition.retention)) {
    if (!Number.isSafeInteger(actual) || actual < 0) {
      return yield* invalid(
        definition.name,
        field,
        "a non-negative safe integer",
        actual,
      );
    }
  }
});
