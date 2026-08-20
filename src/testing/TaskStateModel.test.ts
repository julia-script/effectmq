import { describe, expect, test } from "vitest";
import * as Model from "./TaskStateModel.js";

const value = <A>(result: Model.ModelResult<A>): A => {
  if (!result.ok) throw new Error(result.error);
  return result.value;
};

describe("TaskStateModel", () => {
  test("models offer, fenced attempts, retention, settlement, and removal", () => {
    let model = value(Model.offer(Model.make(), { id: "task" }));
    model = value(Model.acquire(model, "task", "lease-a"));
    expect(Model.renew(model, "task", "old-lease")).toEqual({
      error: "LeaseLost",
      ok: false,
    });
    model = value(Model.retain(model, { holder: "holder-a", id: "task" }));
    model = value(Model.succeed(model, "task", "lease-a"));
    expect(Model.remove(model, "task")).toEqual({
      error: "TaskRetained",
      ok: false,
    });
    model = value(Model.release(model, { holder: "holder-a", id: "task" }));
    model = value(Model.remove(model, "task"));
    expect(model.tasks.size).toBe(0);
    Model.assertInvariants(model);
  });

  test("counts handler failures and stalls independently", () => {
    let model = value(Model.offer(Model.make(), { id: "task" }));
    model = value(Model.acquire(model, "task", "lease-a"));
    model = value(
      Model.fail(model, { id: "task", leaseToken: "lease-a", retry: true }),
    );
    model = value(Model.acquire(model, "task", "lease-b"));
    model = value(Model.expire(model, { id: "task", maxStalledCount: 1 }));
    model = value(Model.acquire(model, "task", "lease-c"));
    model = value(Model.expire(model, { id: "task", maxStalledCount: 1 }));

    expect(model.tasks.get("task")).toMatchObject({
      handlerFailureCount: 1,
      stalledAttemptCount: 2,
      state: "failed",
    });
    Model.assertInvariants(model);
  });
});
