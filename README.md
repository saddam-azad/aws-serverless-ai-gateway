# Serverless AI Gateway on AWS

A production-style serverless gateway that authenticates users by tenant/app, authorizes which models they can use, enforces hourly token quotas, caches responses, retries with backoff and falls back to alternate models, streams Bedrock (or simulated) answers over AppSync Events via WebSocket, and records every request for later analysis in Athena/CloudWatch.

## Architecture

```
Client (browser)
  │
  ▼
WAF ──► Cognito ──► AppSync Events (WebSocket, one long-lived connection)
                         │
                         ▼
                      Lambda (Gate Agent)  ── boarding check → allow-list → quota → cache → model
                         │
                         ▼
                    Amazon Bedrock (ConverseStream)
                         │  token stream back
                         ▼
                    AppSync Events ──► Client (via WebSocket)

Side channel (telemetry/evidence)
  Lambda ──► Kinesis Data Firehose ──► S3 (partitioned by day) ──► AWS Glue (partition projection) ──► Athena
         └──────► CloudWatch (EMF metrics, dashboard, alarm)
```

## Airport map (what each piece does)

| Gate | Airport concept | AWS Service | Code |
|---|---|---|---|
| 1 | Perimeter security | AWS WAF (managed rule groups + per-IP rate limit) | [`lib/stack.ts`](lib/stack.ts#L198-L259) `_perimeter()` |
| 2 | Passport control | Amazon Cognito User Pool (`custom:tenant`, `custom:app` claims) | [`lib/stack.ts`](lib/stack.ts#L62-L82) (`Passengers`), [`lambda/gateway/handler.ts`](lambda/gateway/handler.ts#L89-L97) `boardingPass()` |
| 3 | Boarding gate | AWS AppSync Events (requests channel + responses namespace; cross-user subscription denied) | [`lib/stack.ts`](lib/stack.ts#L84-L196) (`BoardingGate`, `requests` namespace, `responses` namespace), [`appsync/responses.js`](appsync/responses.js) (subscription authorization), [`lambda/gateway/publisher.ts`](lambda/gateway/publisher.ts) (event publishing + retry/backoff + delivery resilience) |
| 4 | Gate agent | AWS Lambda (Node.js 26, ESM, ARM64) | [`lib/stack.ts`](lib/stack.ts#L130-L179) (`GateAgent`), [`lambda/gateway/handler.ts`](lambda/gateway/handler.ts) |
| 5 | Baggage scale | DynamoDB (hourly token quota with conditional write + reconciliation) | [`lambda/gateway/policy.ts`](lambda/gateway/policy.ts#L79-L125) (`reserveQuota`, `reconcile`) |
| 6 | Express lane | DynamoDB response cache (tenant-scoped, key includes app/model/system/version/prompt) | [`lambda/gateway/cache.ts`](lambda/gateway/cache.ts) |
| 7 | The flights | Amazon Bedrock ConverseStream with retries (jittered backoff), fallback chain per app/tenant | [`lambda/gateway/models.ts`](lambda/gateway/models.ts) (`generate`, `streamModel`, mock models) |
| 8 | Accounting office → archive → filing → auditors | Firehose → S3 (day-partitioned) → Glue (partition projection) → Athena (saved queries) | [`lib/stack.ts`](lib/stack.ts#L104-L345) `usageCatalog()`; [`queries/`](queries/) (`tokens_by_app.sql`, `slowest_model.sql`, `find_request.sql`, `outcomes_by_tenant.sql`) |
| 9 | Tracking number | `request_id` on every log line/event/usage record; Logs Insights queries | [`lambda/gateway/telemetry.ts`](lambda/gateway/telemetry.ts), [`lib/stack.ts`](lib/stack.ts#L463-L494) `FindRequest`, `SlowestRequests` |
| 10 | Control tower | CloudWatch EMF metrics, dashboard, fallback-rate alarm | [`lib/stack.ts`](lib/stack.ts#L347-L462) `controlTower()` |

## Request flow

1. **Authenticate**: Browser client signs in to Cognito with USER_PASSWORD_AUTH; receives an ID token with `custom:tenant` and `custom:app`.
2. **Establish one long-lived WebSocket**: Client opens a single AppSync Events WebSocket per user and **subscribes once** to `/responses/{sub}/*` (the JS resolver [`appsync/responses.js`](appsync/responses.js#L6-L9) rejects subscriptions to another user's channel).
3. **Submit request**: Client publishes `{request_id, session_id, prompt, model?, max_tokens?, cacheable?}` to `/requests/submit`. AppSync invokes the Gate Agent Lambda asynchronously.
4. **Authorize & gate**: [`handleRequest()`](lambda/gateway/handler.ts#L99-L159) builds a boarding pass, checks prompt/limits, resolves model via tenant/app rules, checks allow-list (403), reserves quota (429 if exceeded), and checks the cache (6). If cache hit: returns full text directly via stream.
5. **Generate with resilience**: [`generate()`](lambda/gateway/models.ts#L99-L148) tries models in the fallback chain; retries retryable Bedrock errors with exponential backoff + full jitter. Mock models (`mock:flaky`, `mock:steady`) simulate throttles and token streaming for load tests.
6. **Stream back**: [`TokenStream`](lambda/gateway/publisher.ts#L138-233) batches tokens (flush immediately on first token for fast TTFT, then every 100ms or 400 chars), assigns sequence numbers (AppSync Events does not guarantee order), and publishes events. Terminal events (`done`, `error`) are sent twice (best-effort delivery resilience). `done` carries the full answer text when small enough, enabling repair of lost chunks.
7. **Persist evidence**: After completion, usage is reconciled against quota, a Firehose usage record is shipped (non-blocking), EMF metrics are emitted via console logs (zero extra API calls on hot path), and structured request logs are written.

**Delivery note**: Publishing retries transient AppSync publish failures (429/5xx) with jittered backoff; non-retryable 4xx fail fast. Publish failures during streaming are logged and dropped (best-effort) rather than failing the request — `done` copies provide repair.

## Configuration

- **Models**: [`config/models.json`](config/models.json) maps aliases (`mock-a`, `mock-b`, `model-a`, `model-b`, `model-c`) to Bedrock model/inference profile IDs or `mock:flaky`/`mock:steady`. Environment `MODEL_MAP` is injected into Lambda at deploy time.
- **Tenants & users**: [`config/tenants.json`](config/tenants.json) defines tenant defaults (quota, default model, allowed_models, fallbacks) with per-app overrides. The `users` array is used by [`scripts/setup.ts`](scripts/setup.ts) to create Cognito test users with `custom:tenant`/`custom:app`.
- **CDK context** (in [`cdk.json`](cdk.json)): `waf` (bool, default `"true"`), `wafRateLimitPerMinute` (number, default `10000`), `mockThrottleRate` (string, default `"0.05"`). Pass `-c mockThrottleRate=0.6` to simulate a struggling primary model.

## Prerequisites

- [Bun](https://bun.sh/) 1.2+ (package manager and script runner)
- Node.js 26+ (native TypeScript support). This repo includes [`.mise.toml`](.mise.toml) pinning `node = "26"`. Run `mise install` if using mise.
- AWS CLI v2 configured (`aws sts get-caller-identity` must succeed)
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) for load tests (Linux/macOS/Windows)
- **Lambda concurrency** (recommended): check `aws lambda get-account-settings --query AccountLimit.ConcurrentExecutions`. New accounts may be capped at 10; if low, request increase in Service Quotas (AWS Lambda → Concurrent executions). Otherwise requests queue and tests measure queuing.
- **Real Bedrock models** (optional): if you run with `--model model-a`/`model-b`/`model-c`, ensure those models are enabled in Bedrock → Model access in your target region (Claude Haiku 4.5 and Amazon Nova Lite/Micro as configured).

## Quick start

```bash
# Install deps
bun install

# Typecheck (no emit) and unit tests
bun run typecheck  # tsc --noEmit -p tsconfig.json
bun run test       # node --test --test-reporter=spec "tests/unit/*.test.ts" (14 tests)

# Synthesize CloudFormation (no deploy)
bun run synth

# Bootstrap (once per account/region) and deploy
bunx cdk bootstrap
bunx cdk deploy    # or: bun run deploy

# Seed tenants/users and write local browser/loadtest config
bun run setup      # writes client/config.js and loadtest/config.json (gitignored)
```

`scripts/setup.ts` ([code](scripts/setup.ts)) reuses an existing password if `loadtest/config.json` already exists, otherwise generates one. It stores the generated password in both `loadtest/config.json` and `client/config.js` (both gitignored). The client runs entirely in the browser and never handles AWS credentials directly — it authenticates via Cognito USER_PASSWORD_AUTH and passes the ID token to AppSync Events.

## Running tests

### Unit tests (offline)

```bash
bun run test  # node --test, 14 unit tests
```

Covers: cache key tenant isolation and normalization ([cache.test.ts](tests/unit/cache.test.ts)), model retry/backoff/fallback/stream interruption ([models.test.ts](tests/unit/models.test.ts)), quota atomics/estimation/app overrides ([policy.test.ts](tests/unit/policy.test.ts)), publish retry semantics (throttle/retryable vs client errors) and TokenStream batching/ordering/`done` duplication ([publisher.test.ts](tests/unit/publisher.test.ts)).

### End-to-end (against deployed stack)

Requires a deployed stack and `client/config.js`/`loadtest/config.json` from `setup`.

```bash
bun run e2e
```

[`tests/e2e.ts`](tests/e2e.ts) opens one long-lived WebSocket, subscribes once to `/responses/{sub}/*`, and verifies: (1) normal streamed answer (tokens + `done`), (2) response cache (miss then hit for the same cacheable prompt), (3) quota enforcement (fabrikam → 429 once over quota), (4) allow-list (contoso-knowledge cannot use `model-a` → 403).

## Load tests (k6)

From the `loadtest/` directory:

```bash
cd loadtest
./run.sh baseline                 # 10 users × 10 requests (mock models)
./run.sh load                     # ramp 10 → 50 → 100 → 500 concurrent users
./run.sh burst                    # step 10 → 100 → 500 req/s
./run.sh streaming                # 50 users, large output; checks for truncation
./run.sh quota                     # fabrikam pushed over quota (expect many 429s)
./run.sh policy                   # contoso-knowledge + model-a (expect many 403s)
./run.sh baseline --model model-a # real Bedrock run
```

Options: `--model <alias>`, `--load-max <n>` (load scenario peak, default 500), `--burst-max <n>` (burst peak, default 500), `--vus <n>` (override for streaming/quota).

Each run writes:
- `loadtest/results/<name>.json` (k6 summary export)
- `loadtest/results/<name>.html` (k6 web dashboard HTML with charts)

**Custom metrics** ([gateway.js](loadtest/gateway.js#L23-L36)): `gw_ttft`, `gw_latency`, `gw_tokens_per_sec`, `gw_success`, `gw_quota_429`, `gw_denied_403`, `gw_truncated`, `gw_cache_hits`, `gw_fallbacks`, `gw_unexpected`, plus delivery metrics `gw_events_expected`, `gw_events_lost`, `gw_answers_repaired`.

With a low Lambda concurrency limit, pass `--load-max 100`/`--burst-max 100` to avoid over-saturating and measuring only queuing.

### Simulate a struggling primary model

```bash
bunx cdk deploy -c mockThrottleRate=0.6  # ~20% of mock-flaky calls throttle
cd loadtest && ./run.sh load --load-max 100
```

Watch the fallback-rate panel in the CloudWatch dashboard and the `ai-gateway-fallback-rate-high` alarm. Redeploy without the context flag to reset.

## Browser demo client

After `bun run setup`:

```bash
cd client
python3 -m http.server 8000  # or: bunx serve .
# open http://localhost:8000/
```

Features:
- Picks a demo user (from `config.js`) and tenant/app model
- Establishes one long-lived WebSocket + one wildcard subscription (reused per request)
- Reorders token chunks by `seq` (AppSync Events not guaranteed order)
- Renders TTFT, total latency, model used, tokens in/out, cache/fallback status
- `done` carries full text to repair any lost chunks; `done` is sent twice for resilience
- Try `contoso-knowledge` + `model-a` → 403; tick **Cacheable** and send identical prompt twice → cache hit

Source: [`client/index.html`](client/index.html), [`client/app.js`](client/app.js), [`client/config.js`](client/config.js) (generated).

## Evidence and observability

| Question | Where to look |
|---|---|
| p95/p99 latency, TTFT, throttle/fallback/cache rates, Lambda concurrency | CloudWatch dashboard `ai-gateway-control-tower` (URL printed by `setup`) |
| "Who used tokens today?" | Athena workgroup `ai-gateway` → saved query **tokens by app** ([`tokens_by_app.sql`](queries/tokens_by_app.sql)) |
| "Which model is slowest?" | **slowest model** ([`slowest_model.sql`](queries/slowest_model.sql), uses percentiles) |
| "Why did request X take 38s?" | Logs Insights `ai-gateway/find-request-by-id` or Athena **find request** ([`find_request.sql`](queries/find_request.sql)) |
| Outcomes by tenant (429/403/cache/fallback/errors) | **outcomes by tenant** ([`outcomes_by_tenant.sql`](queries/outcomes_by_tenant.sql)) |
| Quota counters | DynamoDB table (`StateTableName` output) → items `QUOTA#<tenant>#<yyyymmddhh>` |

Usage records land in S3 in batches (Firehose `bufferingHints`: 60s/5MB), partitioned by `day=YYYY-MM-DD` with partition projection enabled (no crawler required).

## Important implementation details

- **Native TypeScript (Node 26)**: Relative imports include `.ts` extensions (ESM). Code uses erasable syntax only (no `enum`, `namespace`, constructor parameter properties) — enforced by `erasableSyntaxOnly: true` in [`tsconfig.json`](tsconfig.json). No build/emit step; Node 26 strips types at runtime.
- **Lambda bundling**: [`lib/stack.ts`](lib/stack.ts#L135-L150) uses `NodejsFunction` with `nodejs26.x` (ARM64), `esbuild` minification, and `bundling.bundleAwsSDK: true` to bundle `@aws-sdk/*` and `@smithy/*` into the deployed asset (no runtime SDK dependency on the Lambda Node image). Bundling is handled by CDK/esbuild during `cdk deploy` (no separate build script).
- **AppSync Events publish**: Raw SigV4-signed HTTP `POST` to `/event` using [`@smithy/signature-v4`](lambda/gateway/publisher.ts#L49-88) + global `fetch`. Retry semantics, jittered backoff, and best-effort delivery match the reference design ([`publisher.ts`](lambda/gateway/publisher.ts)).
- **Testability seams**: `generate(..., options { streamFn, sleep })`, `TokenStream(..., options { publish, clock })`, `publishEvents(..., options { post, sleep, attempts })` enable deterministic unit tests.
- **Delivery resilience**: One long-lived WebSocket + one wildcard subscription per user; chunk sequence numbers; `done` carries full text; `done`/`error` sent twice; best-effort publish drops on persistent failures.
- **Non-blocking side effects**: Usage record shipping catches and logs failures without failing the passenger request; publish failures are logged and never thrown from `TokenStream`.
- **WAF toggle**: Set `-c waf=false` on deploy if your region doesn't support WAF on AppSync Event APIs (see Troubleshooting).

## Development commands

```bash
bun run typecheck  # TypeScript type checking only
bun run test       # Run all unit tests (node --test)
bun run synth      # CDK synth
bun run diff       # CDK diff
bun run deploy     # CDK deploy
bun run destroy    # CDK destroy
bun run setup      # Seed tenants/users + write local config
bun run e2e        # End-to-end against deployment
bun run loadtest   # cd loadtest && ./run.sh (pass scenario as args)
```

## Project layout

```text
bin/app.ts                        CDK app entry point
lib/stack.ts                      Stack: WAF, Cognito, AppSync Events, Lambda, Firehose/Glue/Athena, CloudWatch
lambda/gateway/handler.ts         Gate agent (boarding pass, request handling, streaming orchestration)
lambda/gateway/policy.ts          Tenant/app rules, model allow-list, quotas (reserve/reconcile)
lambda/gateway/cache.ts           Response cache (tenant-scoped)
lambda/gateway/models.ts          Bedrock ConverseStream + mocks, retry/backoff, fallback
lambda/gateway/publisher.ts       AppSync Events publishing, batching, delivery resilience
lambda/gateway/telemetry.ts       EMF metrics, structured logs, Firehose usage records
appsync/responses.js              AppSync resolver: deny cross-user subscriptions
config/models.json                Model alias → Bedrock/mock mapping
config/tenants.json               Tenants, apps, quotas, fallbacks, test users
queries/*.sql                     Saved Athena queries (4)
scripts/setup.ts                  Seed DynamoDB/Cognito, write client/loadtest config
client/index.html                 Browser demo UI
client/app.js                     Browser demo logic (WebSocket streaming)
loadtest/gateway.js               k6 load test script (WebSocket + metrics)
loadtest/run.sh                   k6 runner wrapper
tests/unit/*.test.ts              Unit tests (node --test)
tests/e2e.ts                      End-to-end tests against deployed stack
tsconfig.json                     TypeScript config (erasableSyntaxOnly, allowImportingTsExtensions)
.mise.toml                        Tool versions (Node 26)
cdk.json                          CDK app + context defaults
```

## Troubleshooting

- **WAF association fails on deploy**: Some regions may not support WAF on AppSync Event APIs. Deploy with `-c waf=false` (`bunx cdk deploy -c waf=false`).
- **Model access errors** (`AccessDeniedException` in Gate Agent logs): Enable required models (Claude Haiku 4.5, Amazon Nova Lite/Micro) in Bedrock → Model access for your region, or stick to mock models (`mock-a`/`mock-b`).
- **Latency climbs linearly as users increase**: Check CloudWatch "Gate agent (Lambda) concurrency and throttles" widget. If concurrency is pinned to your account limit, requests are queuing for Lambda (increase concurrency quota or reduce load).
- **Client shows "Missing config.js"**: Run `bun run setup` after `bunx cdk deploy`. `client/config.js` is gitignored and generated from stack outputs.
- **E2E/quota test takes time**: The quota test may issue up to ~40 requests to exhaust `fabrikam`'s small hourly quota; increase timeout if needed (test uses up to 120s per request wait).
- **Athena queries return empty for recent requests**: Firehose buffers (60s/5MB) before writing to S3; wait ~60s after a request before querying. Partition projection means new days are immediately queryable.

## Notes

This implementation preserves drop-in behavioral compatibility where relevant (4 saved Athena queries: `tokens_by_app`, `slowest_model`, `find_request`, `outcomes_by_tenant`; `ai_usage` Glue table schema/partitioning by day with partition projection; CloudWatch metrics namespace `AIGateway`; EMF log format). The AppSync resolver remains JavaScript (`appsync/responses.js`) as required by the AppSync resolver runtime. No build step is required anywhere; everything runs natively with the configured toolchain.