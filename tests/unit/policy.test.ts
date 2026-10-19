import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { appPolicy, authorized, estimateTokens, fallbackChain, pickModel } from "../../lambda/gateway/policy.ts";

const CONTOSO = {
  token_quota_per_hour: 1000,
  default_model: "mock-a",
  allowed_models: ["mock-a", "mock-b", "model-a"],
  fallbacks: { "mock-a": ["mock-b"], "model-a": ["model-b"] },
  apps: { "knowledge-bot": { default_model: "mock-b", allowed_models: ["mock-b"] } },
};

describe("tenant and app policy", () => {
  it("lets an app override narrow the tenant's models", () => {
    const rules = appPolicy(CONTOSO, "knowledge-bot");
    assert.equal(pickModel(rules, null), "mock-b");
    assert.equal(authorized(rules, "model-a"), false);
    assert.equal(authorized(appPolicy(CONTOSO, "support-assistant"), "model-a"), true);
  });

  it("skips fallbacks the app is not allowed to use", () => {
    const rules = appPolicy(CONTOSO, "support-assistant");
    assert.deepEqual(fallbackChain(rules, "mock-a"), ["mock-a", "mock-b"]);
    assert.deepEqual(fallbackChain(rules, "model-a"), ["model-a"]); // model-b not allowed
  });

  it("counts the prompt plus the output ceiling", () => {
    assert.equal(estimateTokens("x".repeat(400), 300), 400);
  });
});
