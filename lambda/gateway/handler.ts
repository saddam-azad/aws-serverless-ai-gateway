/**
 * Gate 4, the gate agent: every AI request passes this desk before it boards a model.
 *
 * AppSync Events invokes this function asynchronously for each publish to
 * /requests/submit. The answer never goes back through the return value: tokens
 * are published to /responses/{sub}/{request_id}, which only the caller can
 * subscribe to.
 */
import * as cache from "./cache.ts";
import { AllModelsBusy, StreamInterrupted, generate } from "./models.ts";
import * as policy from "./policy.ts";
import { TokenStream } from "./publisher.ts";
import * as telemetry from "./telemetry.ts";

const SYSTEM_PROMPT = process.env.SYSTEM_PROMPT ?? "You are a concise, helpful assistant.";
const SYSTEM_PROMPT_VERSION = process.env.SYSTEM_PROMPT_VERSION ?? "v1";
const MAX_PROMPT_CHARS = 20_000;
const MAX_OUTPUT_TOKENS = 4096;
const DEFAULT_OUTPUT_TOKENS = 512;
const REQUEST_ID = /^[A-Za-z0-9-]{8,48}$/;

export class GatewayError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "GatewayError";
    this.status = status;
  }
}

interface Passenger {
  sub: string | undefined;
  user: string | undefined;
  tenant: string | undefined;
  app: string | undefined;
}

interface UsageRecord {
  request_id: string;
  tenant: string | undefined;
  app: string | undefined;
  user_id: string | undefined;
  requested_model: string | null;
  model: string | null;
  status: number | null;
  input_tokens: number;
  output_tokens: number;
  ttft_ms: number | null;
  latency_ms: number | null;
  cache: string;
  fallback: boolean;
  retries: number;
  throttles: number;
  error: string | null;
  timestamp: string;
}

export interface AppSyncIdentity {
  sub?: string;
  username?: string;
  claims?: Record<string, string | undefined>;
}

interface AppSyncEvent {
  identity?: AppSyncIdentity;
  events?: { payload?: unknown }[];
}

export async function handler(event: AppSyncEvent): Promise<void> {
  const identity = event.identity ?? {};
  for (const item of event.events ?? []) {
    let payload: unknown = item.payload;
    if (typeof payload === "string") {
      try {
        payload = JSON.parse(payload);
      } catch {
        payload = null;
      }
    }
    await handleRequest(
      identity,
      payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {},
    );
  }
}

/** Gate 2: turn the verified Cognito identity into a boarding pass. */
export function boardingPass(identity: AppSyncIdentity): Passenger {
  const claims = identity.claims ?? {};
  return {
    sub: identity.sub ?? claims.sub,
    user: identity.username ?? claims["cognito:username"],
    tenant: claims["custom:tenant"],
    app: claims["custom:app"],
  };
}

export async function handleRequest(
  identity: AppSyncIdentity,
  payload: Record<string, unknown>,
): Promise<void> {
  const passenger = boardingPass(identity);
  const requestId = String(payload.request_id ?? "");
  if (!passenger.sub || !REQUEST_ID.test(requestId)) {
    // Without a caller and a request ID there is no channel to answer on.
    telemetry.log("unroutable", requestId || null, { reason: "missing sub or invalid request_id" });
    return;
  }

  // A client session (one tab, one device) subscribes once to
  // /responses/{sub}/{session_id}/*, so two sessions of the same user never
  // receive each other's answers.
  const sessionId = String(payload.session_id ?? "");
  const channel =
    sessionId && REQUEST_ID.test(sessionId)
      ? `/responses/${passenger.sub}/${sessionId}/${requestId}`
      : `/responses/${passenger.sub}/${requestId}`;

  const stream = new TokenStream(channel, requestId);
  const record: UsageRecord = {
    request_id: requestId,
    tenant: passenger.tenant,
    app: passenger.app,
    user_id: passenger.user,
    requested_model: null,
    model: null,
    status: null,
    input_tokens: 0,
    output_tokens: 0,
    ttft_ms: null,
    latency_ms: null,
    cache: "skip",
    fallback: false,
    retries: 0,
    throttles: 0,
    error: null,
    timestamp: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
  };
  telemetry.log("received", requestId, { tenant: record.tenant, app: record.app });

  try {
    await serve(passenger, payload, stream, record);
    record.status = 200;
  } catch (err) {
    if (err instanceof GatewayError) {
      record.status = err.status;
      record.error = err.message;
      await safe(() => stream.error(err.status, err.message));
    } else {
      record.status = 500;
      record.error = err instanceof Error ? err.name : "Error";
      telemetry.logException("unhandled", requestId);
      await safe(() => stream.error(500, "Internal gateway error"));
    }
  } finally {
    await finish(record, stream);
  }
}

async function serve(
  passenger: Passenger,
  payload: Record<string, unknown>,
  stream: TokenStream,
  record: UsageRecord,
): Promise<void> {
  const requestId = record.request_id;
  const { tenant, app } = passenger;
  if (!tenant || !app) throw new GatewayError(401, "Token has no tenant or app claim");

  const tenantCfg = await policy.tenantConfig(tenant);
  if (!tenantCfg) throw new GatewayError(403, `Unknown tenant ${tenant}`);
  const rules = policy.appPolicy(tenantCfg, app);

  const prompt = payload.prompt;
  if (typeof prompt !== "string" || !prompt.trim()) throw new GatewayError(400, "prompt is required");
  if (prompt.length > MAX_PROMPT_CHARS) {
    throw new GatewayError(413, `prompt is longer than ${MAX_PROMPT_CHARS} characters`);
  }

  const rawMax = payload.max_tokens ?? DEFAULT_OUTPUT_TOKENS;
  const numericMax = typeof rawMax === "number" ? rawMax : Number(rawMax);
  if (!Number.isFinite(numericMax)) throw new GatewayError(400, "max_tokens must be a number");
  const maxTokens = Math.min(Math.max(Math.trunc(numericMax), 1), MAX_OUTPUT_TOKENS);

  // Is this passenger allowed on this flight?
  const model = policy.pickModel(rules, payload.model as string | undefined);
  record.requested_model = model;
  if (!policy.authorized(rules, model)) {
    throw new GatewayError(403, `Model ${model} is not allowed for ${tenant}/${app}`);
  }

  // Gate 5, the baggage scale: reserve an estimate before boarding.
  const estimate = policy.estimateTokens(prompt, maxTokens);
  const limit = Math.trunc(rules.token_quota_per_hour as number);
  const reservation = await policy.reserveQuota(tenant, estimate, limit);
  if (reservation === null) throw new GatewayError(429, "Token quota exceeded");
  telemetry.emitMetrics(
    { QuotaUtilization: Math.round(((100 * reservation.used) / limit) * 100) / 100 },
    { Tenant: tenant },
  );
  telemetry.log("quota_reserved", requestId, { estimate, used: reservation.used, limit });

  let charged = 0;
  try {
    // Gate 6, the express lane. Only callers who mark a prompt cacheable use it,
    // because personal or time-sensitive answers must never be shared.
    let key: string | null = null;
    if (payload.cacheable === true) {
      key = cache.cacheKey(tenant, app, model, SYSTEM_PROMPT_VERSION, prompt);
      const cached = await cache.lookup(key);
      if (cached !== null) {
        record.cache = "hit";
        record.model = model;
        await stream.text(cached);
        await stream.done(doneSummary(record));
        return;
      }
      record.cache = "miss";
    }

    // Gate 7, takeoff: retry with backoff, then fall back to the next model.
    try {
      const result = await generate(
        policy.fallbackChain(rules, model),
        prompt,
        SYSTEM_PROMPT,
        maxTokens,
        (chunk) => stream.text(chunk),
      );
      charged = result.input_tokens + result.output_tokens;
      record.model = result.model;
      record.input_tokens = result.input_tokens;
      record.output_tokens = result.output_tokens;
      record.fallback = result.fallback;
      record.retries = result.retries;
      record.throttles = result.throttles;
      if (key) await cache.store(key, stream.fullText, model);
      await stream.done(doneSummary(record));
    } catch (err) {
      if (err instanceof AllModelsBusy) {
        record.retries = err.retries;
        record.throttles = err.throttles;
        throw new GatewayError(503, "All models are busy, try again shortly");
      }
      if (err instanceof StreamInterrupted) {
        record.model = err.model;
        record.retries = err.retries;
        record.throttles = err.throttles;
        charged = estimate; // the model did work we cannot measure
        throw new GatewayError(502, "The model stream was interrupted");
      }
      throw err;
    }
  } finally {
    // Actual usage only exists after the flight: correct the estimate.
    await policy.reconcile(reservation, charged);
  }
}

function doneSummary(record: UsageRecord): Record<string, unknown> {
  return {
    model: record.model,
    cache: record.cache,
    fallback: record.fallback,
    retries: record.retries,
    input_tokens: record.input_tokens,
    output_tokens: record.output_tokens,
  };
}

async function finish(record: UsageRecord, stream: TokenStream): Promise<void> {
  const latencyMs = stream.elapsedMs();
  const ttftMs = stream.ttftMs();
  record.latency_ms = latencyMs;
  record.ttft_ms = ttftMs;
  const status = record.status;

  let generationSeconds: number | null = null;
  if (ttftMs !== null && record.output_tokens) {
    generationSeconds = Math.max(latencyMs - ttftMs, 1) / 1000;
  }
  telemetry.emitMetrics({
    Requests: 1,
    Latency: latencyMs,
    TTFT: ttftMs,
    TokensPerSecond:
      generationSeconds !== null ? Math.round((record.output_tokens / generationSeconds) * 10) / 10 : null,
    InputTokens: record.input_tokens,
    OutputTokens: record.output_tokens,
    CacheHits: record.cache === "hit" ? 1 : 0,
    CacheMisses: record.cache === "miss" ? 1 : 0,
    Fallbacks: record.fallback ? 1 : 0,
    Retries: record.retries,
    Throttles: record.throttles,
    QuotaRejections: status === 429 ? 1 : 0,
    Denied: status === 401 || status === 403 ? 1 : 0,
    Errors: status === null || status >= 500 ? 1 : 0,
  });
  await telemetry.shipUsageRecord(record as unknown as Record<string, unknown>);
  const { request_id: requestId, ...rest } = record;
  telemetry.log("completed", requestId, rest as Record<string, unknown>);
}

async function safe(fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
  } catch {
    telemetry.logException("publish_failed", null);
  }
}
