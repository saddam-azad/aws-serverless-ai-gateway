import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  AllModelsBusy,
  ModelThrottled,
  StreamInterrupted,
  generate,
  type StreamFn,
  type StreamPiece,
} from "../../lambda/gateway/models.ts";

type Outcome = "ok" | "throttle" | "die-midstream";

/** behaviour: alias -> list of outcomes per call: "ok", "throttle", or "die-midstream". */
function fakeModels(behaviour: Record<string, Outcome[]>): StreamFn {
  const calls: Record<string, Outcome[]> = Object.fromEntries(
    Object.entries(behaviour).map(([alias, outcomes]) => [alias, [...outcomes]]),
  );
  return async function* (alias: string): AsyncGenerator<StreamPiece> {
    const outcome = calls[alias].shift();
    if (outcome === "throttle") throw new ModelThrottled(alias);
    yield ["text", "hello "];
    if (outcome === "die-midstream") throw new ModelThrottled(alias);
    yield ["usage", { input_tokens: 3, output_tokens: 1 }];
  };
}

const noSleep = async (): Promise<void> => {};

describe("generate", () => {
  it("retries a throttle, then succeeds on the primary", async () => {
    const out: string[] = [];
    const result = await generate(
      ["a", "b"],
      "p",
      "s",
      5,
      (text) => {
        out.push(text);
      },
      {
        streamFn: fakeModels({ a: ["throttle", "ok"] }),
        sleep: noSleep,
      },
    );
    assert.deepEqual([result.model, result.fallback, result.retries, result.throttles], ["a", false, 1, 1]);
    assert.deepEqual(out, ["hello "]);
  });

  it("falls back only after retries are exhausted", async () => {
    const result = await generate(["a", "b"], "p", "s", 5, () => {}, {
      streamFn: fakeModels({ a: ["throttle", "throttle", "throttle"], b: ["ok"] }),
      sleep: noSleep,
    });
    assert.deepEqual([result.model, result.fallback, result.throttles], ["b", true, 3]);
  });

  it("raises AllModelsBusy when every model is throttled", async () => {
    await assert.rejects(
      generate(["a"], "p", "s", 5, () => {}, {
        streamFn: fakeModels({ a: ["throttle", "throttle", "throttle"] }),
        sleep: noSleep,
      }),
      AllModelsBusy,
    );
  });

  it("never switches model mid-answer", async () => {
    await assert.rejects(
      generate(["a", "b"], "p", "s", 5, () => {}, {
        streamFn: fakeModels({ a: ["die-midstream"], b: ["ok"] }),
        sleep: noSleep,
      }),
      StreamInterrupted,
    );
  });
});
