import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { cacheKey } from "../../lambda/gateway/cache.ts";

describe("response cache key", () => {
  it("never crosses tenants and ignores case or extra spacing", () => {
    const a = cacheKey("contoso", "support", "mock-a", "v1", "What's your refund policy?");
    const b = cacheKey("fabrikam", "support", "mock-a", "v1", "What's your refund policy?");
    assert.notEqual(a, b);
    assert.equal(a, cacheKey("contoso", "support", "mock-a", "v1", "  what's your REFUND policy? "));
    assert.notEqual(a, cacheKey("contoso", "support", "mock-a", "v2", "What's your refund policy?"));
  });
});
