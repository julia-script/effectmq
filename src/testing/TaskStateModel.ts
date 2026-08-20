/**
 * Small executable reference model for queue-state property tests.
 *
 * It deliberately contains no Redis or Effect behavior. Tests apply the same
 * operation sequence to this model and the real engine, then compare the
 * externally observable state and invariants.
 */

export type ExecutionState =
  | "delayed"
  | "waiting"
  | "leased"
  | "retry-scheduled"
  | "succeeded"
  | "failed";

export interface ModelTask {
  readonly id: string;
  readonly generation: number;
  readonly state: ExecutionState;
  readonly leaseToken?: string;
  readonly handlerFailureCount: number;
  readonly stalledAttemptCount: number;
  readonly retainedBy: ReadonlySet<string>;
}

export interface TaskStateModel {
  readonly tasks: ReadonlyMap<string, ModelTask>;
}

export type ModelError =
  | "TaskNotFound"
  | "TaskAlreadyExists"
  | "TaskNotRunnable"
  | "LeaseLost"
  | "TaskNotSettled"
  | "TaskRetained";

export type ModelResult<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly error: ModelError };

const ok = <A>(value: A): ModelResult<A> => ({ ok: true, value });
const error = <A = never>(reason: ModelError): ModelResult<A> => ({
  ok: false,
  error: reason,
});

export const make = (): TaskStateModel => ({ tasks: new Map() });

const replace = (model: TaskStateModel, task: ModelTask): TaskStateModel => {
  const tasks = new Map(model.tasks);
  tasks.set(task.id, task);
  return { tasks };
};

const find = (model: TaskStateModel, id: string): ModelResult<ModelTask> => {
  const task = model.tasks.get(id);
  return task === undefined ? error("TaskNotFound") : ok(task);
};

export const offer = (
  model: TaskStateModel,
  options: {
    readonly id: string;
    readonly generation?: number;
    readonly delayed?: boolean;
  },
): ModelResult<TaskStateModel> => {
  if (model.tasks.has(options.id)) return error("TaskAlreadyExists");
  return ok(
    replace(model, {
      id: options.id,
      generation: options.generation ?? 1,
      state: options.delayed ? "delayed" : "waiting",
      handlerFailureCount: 0,
      stalledAttemptCount: 0,
      retainedBy: new Set(),
    }),
  );
};

export const acquire = (
  model: TaskStateModel,
  id: string,
  leaseToken: string,
): ModelResult<TaskStateModel> => {
  const found = find(model, id);
  if (!found.ok) return found;
  if (
    found.value.state !== "waiting" &&
    found.value.state !== "retry-scheduled"
  ) {
    return error("TaskNotRunnable");
  }
  return ok(
    replace(model, {
      ...found.value,
      state: "leased",
      leaseToken,
    }),
  );
};

const currentLease = (
  model: TaskStateModel,
  id: string,
  leaseToken: string,
): ModelResult<ModelTask> => {
  const found = find(model, id);
  if (!found.ok) return found;
  return found.value.state === "leased" && found.value.leaseToken === leaseToken
    ? found
    : error("LeaseLost");
};

export const renew = (
  model: TaskStateModel,
  id: string,
  leaseToken: string,
): ModelResult<TaskStateModel> => {
  const found = currentLease(model, id, leaseToken);
  return found.ok ? ok(model) : found;
};

export const fail = (
  model: TaskStateModel,
  options: {
    readonly id: string;
    readonly leaseToken: string;
    readonly retry: boolean;
  },
): ModelResult<TaskStateModel> => {
  const found = currentLease(model, options.id, options.leaseToken);
  if (!found.ok) return found;
  return ok(
    replace(model, {
      ...found.value,
      state: options.retry ? "retry-scheduled" : "failed",
      leaseToken: undefined,
      handlerFailureCount: found.value.handlerFailureCount + 1,
    }),
  );
};

export const succeed = (
  model: TaskStateModel,
  id: string,
  leaseToken: string,
): ModelResult<TaskStateModel> => {
  const found = currentLease(model, id, leaseToken);
  if (!found.ok) return found;
  return ok(
    replace(model, {
      ...found.value,
      state: "succeeded",
      leaseToken: undefined,
    }),
  );
};

export const expire = (
  model: TaskStateModel,
  options: { readonly id: string; readonly maxStalledCount: number },
): ModelResult<TaskStateModel> => {
  const found = find(model, options.id);
  if (!found.ok) return found;
  if (found.value.state !== "leased") return error("TaskNotRunnable");
  const stalledAttemptCount = found.value.stalledAttemptCount + 1;
  return ok(
    replace(model, {
      ...found.value,
      state:
        stalledAttemptCount > options.maxStalledCount ? "failed" : "waiting",
      leaseToken: undefined,
      stalledAttemptCount,
    }),
  );
};

export const retain = (
  model: TaskStateModel,
  options: { readonly id: string; readonly holder: string },
): ModelResult<TaskStateModel> => {
  const found = find(model, options.id);
  if (!found.ok) return found;
  return ok(
    replace(model, {
      ...found.value,
      retainedBy: new Set([...found.value.retainedBy, options.holder]),
    }),
  );
};

export const release = (
  model: TaskStateModel,
  options: { readonly id: string; readonly holder: string },
): ModelResult<TaskStateModel> => {
  const found = find(model, options.id);
  if (!found.ok) return found;
  const retainedBy = new Set(found.value.retainedBy);
  retainedBy.delete(options.holder);
  return ok(replace(model, { ...found.value, retainedBy }));
};

export const remove = (
  model: TaskStateModel,
  id: string,
): ModelResult<TaskStateModel> => {
  const found = find(model, id);
  if (!found.ok) return found;
  if (found.value.state !== "succeeded" && found.value.state !== "failed") {
    return error("TaskNotSettled");
  }
  if (found.value.retainedBy.size > 0) return error("TaskRetained");
  const tasks = new Map(model.tasks);
  tasks.delete(id);
  return ok({ tasks });
};

export const assertInvariants = (model: TaskStateModel): void => {
  for (const [id, task] of model.tasks) {
    if (id !== task.id) throw new Error(`Task key mismatch for ${id}`);
    if (task.generation < 1) throw new Error(`Invalid generation for ${id}`);
    if (task.state === "leased" && task.leaseToken === undefined) {
      throw new Error(`Leased task ${id} has no token`);
    }
    if (task.state !== "leased" && task.leaseToken !== undefined) {
      throw new Error(`Non-leased task ${id} retains a token`);
    }
    if (task.handlerFailureCount < 0 || task.stalledAttemptCount < 0) {
      throw new Error(`Task ${id} has a negative attempt counter`);
    }
  }
};
