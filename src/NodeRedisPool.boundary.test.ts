import { EventEmitter } from "node:events";
import { expect, it } from "@effect/vitest";
import { vi } from "vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

const redisMock = vi.hoisted(() => ({
  clients: [] as Array<unknown>,
}));

vi.mock("redis", () => ({
  RESP_TYPES: { BLOB_STRING: "blob", MAP: "map" },
  createClientPool: () => redisMock.clients.shift(),
  createSentinel: () => redisMock.clients.shift(),
}));

import * as NodeRedisPool from "./NodeRedisPool.js";

class FakeClient extends EventEmitter {
  readonly calls = { close: 0, connect: 0, destroy: 0 };

  constructor(
    private readonly options: {
      readonly connectFailure?: unknown;
      readonly closeFailure?: unknown;
    } = {},
  ) {
    super();
  }

  connect(): Promise<void> {
    this.calls.connect += 1;
    return this.options.connectFailure === undefined
      ? Promise.resolve()
      : Promise.reject(this.options.connectFailure);
  }

  close(): Promise<void> {
    this.calls.close += 1;
    return this.options.closeFailure === undefined
      ? Promise.resolve()
      : Promise.reject(this.options.closeFailure);
  }

  destroy(): void {
    this.calls.destroy += 1;
  }

  sendCommand(command: ReadonlyArray<string | Buffer>): Promise<unknown> {
    if (command[0] === "INFO") return Promise.resolve("cluster_enabled:0\r\n");
    if (command[0] === "PING") return Promise.resolve("PONG");
    return Promise.resolve(null);
  }
}

it.effect(
  "removes listeners and force-destroys after rejected graceful close",
  () =>
    Effect.gen(function* () {
      const clients = [
        new FakeClient({ closeFailure: new Error("close rejected") }),
        new FakeClient(),
        new FakeClient(),
      ];
      redisMock.clients = [...clients];

      yield* Effect.scoped(Layer.build(NodeRedisPool.layer()));

      for (const client of clients) {
        expect(client.listenerCount("error")).toBe(0);
        expect(client.listenerCount("reconnecting")).toBe(0);
        expect(client.calls.close).toBe(1);
      }
      expect(clients[0].calls.destroy).toBe(1);
      expect(clients[1].calls.destroy).toBe(0);
      expect(clients[2].calls.destroy).toBe(0);
    }),
);

it.effect("releases every acquired client after partial pool acquisition", () =>
  Effect.gen(function* () {
    const first = new FakeClient();
    const second = new FakeClient({
      connectFailure: new Error("connect failed"),
    });
    const third = new FakeClient();
    redisMock.clients = [first, second, third];

    const exit = yield* Effect.scoped(Layer.build(NodeRedisPool.layer())).pipe(
      Effect.exit,
    );

    expect(exit._tag).toBe("Failure");
    expect(first.calls.close).toBe(1);
    expect(second.calls.close).toBe(1);
    expect(third.calls.connect).toBe(0);
    expect(first.listenerCount("error")).toBe(0);
    expect(second.listenerCount("error")).toBe(0);
  }),
);
