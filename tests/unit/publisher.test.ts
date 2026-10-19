import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { PublishError, TokenStream, publishEvents, type Post } from "../../lambda/gateway/publisher.ts";

interface WireEvent {
  type: string;
  text?: string;
  copy?: number;
  status?: number;
  chunks?: number;
  chars?: number;
}

const noSleep = async (): Promise<void> => {};

describe("publishEvents", () => {
  it("retries AppSync throttling, then succeeds", async () => {
    process.env.EVENTS_HTTP_DNS = "events.example.com";
    const calls: number[] = [];
    const post: Post = async () => {
      calls.push(1);
      // Two 429s, then a clean 200.
      return calls.length < 3 ? [429, '{"errorType":"LimitExceededException"}'] : [200, "{}"];
    };
    await publishEvents("/responses/u/r", [{ type: "token" }], { sleep: noSleep, post });
    assert.equal(calls.length, 3);
  });

  it("gives up after the configured attempts", async () => {
    process.env.EVENTS_HTTP_DNS = "events.example.com";
    const calls: number[] = [];
    const post: Post = async () => {
      calls.push(1);
      return [429, '{"errorType":"LimitExceededException"}'];
    };
    await assert.rejects(
      publishEvents("/responses/u/r", [{}], { attempts: 3, sleep: noSleep, post }),
      PublishError,
    );
    assert.equal(calls.length, 3);
  });

  it("fails fast on a client error", async () => {
    process.env.EVENTS_HTTP_DNS = "events.example.com";
    const calls: number[] = [];
    const post: Post = async () => {
      calls.push(1);
      return [403, "forbidden"];
    };
    await assert.rejects(publishEvents("/responses/u/r", [{}], { sleep: noSleep, post }), PublishError);
    assert.equal(calls.length, 1); // a 403 is not worth retrying
  });
});

describe("TokenStream", () => {
  it("sends the first token immediately, then batches", async () => {
    const sent: WireEvent[] = [];
    let now = 0;
    const stream = new TokenStream("/responses/u/r", "r", {
      publish: async (_channel, events) => {
        sent.push(...(events as WireEvent[]));
      },
      clock: () => now,
    });
    await stream.text("A"); // first token: flushed at once for a fast TTFT
    await stream.text("B"); // buffered
    now = 0.2;
    await stream.text("C"); // 200 ms since last flush: flushed with B
    await stream.done({ model: "mock-a" });

    const kinds = sent.map((event) => [event.type, event.text, event.copy]);
    // `done` carries the full answer and is sent twice: delivery is best effort.
    assert.deepEqual(kinds, [
      ["token", "A", undefined],
      ["token", "BC", undefined],
      ["done", "ABC", 1],
      ["done", "ABC", 2],
    ]);
    assert.equal(sent.at(-1)?.chunks, 2);
    assert.equal(sent.at(-1)?.chars, 3);
  });

  it("sends errors twice too", async () => {
    const sent: WireEvent[] = [];
    await new TokenStream("/responses/u/r", "r", {
      publish: async (_channel, events) => {
        sent.push(...(events as WireEvent[]));
      },
    }).error(429, "Token quota exceeded");
    assert.deepEqual(
      sent.map((event) => [event.type, event.status, event.copy]),
      [
        ["error", 429, 1],
        ["error", 429, 2],
      ],
    );
  });

  it("drops failed publishes instead of raising", async () => {
    // Best effort: a publish that fails is logged and counted, never raised, so a
    // throttled chunk cannot turn a completed generation into a 500.
    const sent: WireEvent[] = [];
    let calls = 0;
    const publish = async (_channel: string, events: unknown[]): Promise<void> => {
      calls += 1;
      if (calls === 1) throw new PublishError("AppSync publish returned 429");
      sent.push(...(events as WireEvent[]));
    };
    const stream = new TokenStream("/responses/u/r", "r", { publish });
    await stream.text("A"); // first token flushes immediately, and fails
    await stream.done({ model: "mock-a" }); // still reaches the client
    assert.equal(stream.publishFailures, 1);
    assert.deepEqual(
      sent.map((event) => event.type),
      ["done", "done"],
    );
  });
});
