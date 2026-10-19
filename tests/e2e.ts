/**
 * End-to-end tests against a deployed AI gateway.
 *
 * Unlike tests/unit (offline), these talk to real AWS: they sign in with Cognito,
 * open one long-lived WebSocket to AppSync Events, subscribe to
 * /responses/{sub}/{session}/*, publish to /requests/submit, and read the streamed
 * answer. They cover the four outcomes a client can actually see:
 *
 *   1. a normal streamed answer (tokens + `done`)
 *   2. the response cache (miss then hit for the same cacheable prompt)
 *   3. quota enforcement (a clean 429 once a tenant burns its hourly tokens)
 *   4. the model allow-list (a clean 403 for a model the app may not use)
 *
 * Requires a deployed stack and the local config written by scripts/setup.ts:
 *
 *     cdk deploy
 *     node scripts/setup.ts
 *     node tests/e2e.ts
 *
 * Not part of `node --test`: it needs real AWS and is run by hand.
 */
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import * as path from "node:path";

import {
  CognitoIdentityProviderClient,
  InitiateAuthCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import WebSocket from "ws";

const ROOT = path.resolve(import.meta.dirname, "..");
const CONFIG_PATH = process.env.GATEWAY_CONFIG ?? path.join(ROOT, "loadtest", "config.json");

const STREAM_PROMPT = "Summarize this customer's last five tickets and suggest a next step.";
const FAQ_PROMPT = "What's your refund policy?";

interface GatewayUser {
  username: string;
  tenant: string;
  app: string;
}

interface Config {
  region: string;
  httpDns: string;
  realtimeDns: string;
  clientId: string;
  password: string;
  users: GatewayUser[];
}

interface WireEvent {
  type?: string;
  request_id?: string;
  status?: number;
  message?: string;
  [key: string]: unknown;
}

interface Result {
  id: string;
  outcome: string | number;
  message?: string;
  text?: string;
  [key: string]: unknown;
}

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

function loadConfig(): Config {
  if (!existsSync(CONFIG_PATH)) {
    console.error(`missing ${CONFIG_PATH}; run scripts/setup.ts after deploying`);
    process.exit(1);
  }
  return JSON.parse(readFileSync(CONFIG_PATH, "utf8")) as Config;
}

/** Passport control: exchange a username and password for an ID token. */
async function signIn(cfg: Config, username: string): Promise<{ token: string; sub: string }> {
  const cognito = new CognitoIdentityProviderClient({ region: cfg.region });
  const res = await cognito.send(
    new InitiateAuthCommand({
      AuthFlow: "USER_PASSWORD_AUTH",
      ClientId: cfg.clientId,
      AuthParameters: { USERNAME: username, PASSWORD: cfg.password },
    }),
  );
  const token = res.AuthenticationResult?.IdToken as string;
  const payload = token.split(".")[1];
  const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as { sub: string };
  return { token, sub: claims.sub };
}

/**
 * One connection and one wildcard subscription per user, reused for every request.
 *
 * This is the shape the client is supposed to use: it is what keeps AppSync Events
 * from dropping answers published before a fresh subscription is ready.
 */
class Session {
  private readonly cfg: Config;
  private readonly auth: { host: string; Authorization: string };
  private readonly sub: string;
  private readonly sessionId = randomUUID();
  private readonly events: Record<string, WireEvent[]> = {};
  private readonly done: Record<string, WireEvent[]> = {};
  private readonly errors: Record<string, WireEvent[]> = {};
  private ws!: WebSocket;
  private closed = false;
  private subscribed = false;

  constructor(cfg: Config, token: string, sub: string) {
    this.cfg = cfg;
    this.auth = { host: cfg.httpDns, Authorization: token };
    this.sub = sub;
  }

  private open(): void {
    const header = Buffer.from(JSON.stringify(this.auth)).toString("base64url");
    this.ws = new WebSocket(`wss://${this.cfg.realtimeDns}/event/realtime`, [
      "aws-appsync-event-ws",
      `header-${header}`,
    ]);
    this.ws.on("open", () => this.ws.send(JSON.stringify({ type: "connection_init" })));
    this.ws.on("message", (raw) => this.onMessage(raw.toString()));
    this.ws.on("close", () => {
      this.closed = true;
    });
    this.ws.on("error", (err) => console.log(`  ws error: ${String(err)}`));
  }

  private onMessage(raw: string): void {
    const msg = JSON.parse(raw) as { type?: string; event?: string };
    const kind = msg.type;
    if (kind === "connection_ack") {
      this.ws.send(
        JSON.stringify({
          type: "subscribe",
          id: randomUUID(),
          channel: `/responses/${this.sub}/${this.sessionId}/*`,
          authorization: this.auth,
        }),
      );
    } else if (kind === "subscribe_success") {
      this.subscribed = true;
    } else if (kind === "data" && msg.event !== undefined) {
      let event = JSON.parse(msg.event) as WireEvent | string;
      if (typeof event === "string") event = JSON.parse(event) as WireEvent;
      const requestId = event.request_id as string;
      (this.events[requestId] ??= []).push(event);
      if (event.type === "done") (this.done[requestId] ??= []).push(event);
      else if (event.type === "error") (this.errors[requestId] ??= []).push(event);
    }
  }

  private async waitUntil(predicate: () => boolean, timeoutMs: number, message: string): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (predicate()) return;
      await delay(100);
    }
    throw new Error(message);
  }

  async connect(timeoutMs = 15_000): Promise<void> {
    this.open();
    await this.waitUntil(() => this.subscribed, timeoutMs, "subscribe did not complete in time");
  }

  async request(payload: Record<string, unknown>, timeoutMs = 90_000): Promise<Result> {
    // AppSync closes idle connections; reconnect rather than fail the test.
    if (this.closed) {
      this.closed = false;
      this.subscribed = false;
      this.open();
      await this.waitUntil(() => this.subscribed, 15_000, "reconnect subscribe did not complete");
    }
    const requestId = randomUUID();
    this.ws.send(
      JSON.stringify({
        type: "publish",
        id: randomUUID(),
        channel: "/requests/submit",
        events: [JSON.stringify({ request_id: requestId, session_id: this.sessionId, ...payload })],
        authorization: this.auth,
      }),
    );
    const started = Date.now();
    const deadline = started + timeoutMs;
    while (Date.now() < deadline) {
      const done = this.done[requestId]?.[0];
      if (done) return { id: requestId, outcome: "done", ttft_ms: Date.now() - started, ...done };
      const error = this.errors[requestId]?.[0];
      if (error) return { id: requestId, outcome: error.status ?? "error", message: error.message };
      await delay(50);
    }
    return { id: requestId, outcome: "timeout" };
  }

  chunks(requestId: string): number {
    return (this.events[requestId] ?? []).filter((event) => event.type === "token").length;
  }

  close(): void {
    this.ws.close();
  }
}

/** A normal request streams tokens back and ends with a `done` carrying the answer. */
async function testStreaming(session: Session): Promise<boolean> {
  const result = await session.request({ prompt: STREAM_PROMPT, max_tokens: 60, model: "mock-a" });
  const count = session.chunks(result.id);
  console.log(
    `  outcome=${result.outcome} model=${result.model} fallback=${result.fallback} ` +
      `retries=${result.retries} chunks=${count} ttft=${result.ttft_ms}ms ` +
      `in=${result.input_tokens} out=${result.output_tokens}`,
  );
  return result.outcome === "done" && count > 0 && result.output_tokens === 60;
}

/** A unique cacheable prompt misses, then hits; the answer text is identical. */
async function testCache(session: Session): Promise<boolean> {
  const unique = `${FAQ_PROMPT} (order ${randomUUID()})`;
  const first = await session.request({ prompt: unique, max_tokens: 40, cacheable: true });
  const second = await session.request({ prompt: unique, max_tokens: 40, cacheable: true });
  console.log(`  first: cache=${first.cache} outcome=${first.outcome}`);
  console.log(`  second: cache=${second.cache} outcome=${second.outcome}`);
  return (
    first.outcome === "done" &&
    first.cache === "miss" &&
    second.cache === "hit" &&
    second.text === first.text
  );
}

/** fabrikam's hourly quota is small; keep asking until the gateway answers 429. */
async function testQuota(cfg: Config): Promise<boolean> {
  const { token, sub } = await signIn(cfg, "fabrikam-docs");
  const session = new Session(cfg, token, sub);
  await session.connect();
  try {
    for (let attempt = 1; attempt <= 40; attempt++) {
      const result = await session.request({ prompt: STREAM_PROMPT, max_tokens: 1900, model: "mock-b" }, 120_000);
      if (result.outcome === 429) {
        console.log(`  request ${attempt}: 429 ${JSON.stringify(result.message)}`);
        return true;
      }
      console.log(`  request ${attempt}: ${result.outcome} out=${result.output_tokens}`);
    }
    return false;
  } finally {
    session.close();
  }
}

/** contoso-knowledge may only use cheap models; asking for model-a is a 403. */
async function testPolicy(cfg: Config): Promise<boolean> {
  const { token, sub } = await signIn(cfg, "contoso-knowledge");
  const session = new Session(cfg, token, sub);
  await session.connect();
  try {
    const result = await session.request({ prompt: "Hello", max_tokens: 20, model: "model-a" });
    console.log(`  outcome=${result.outcome} message=${JSON.stringify(result.message)}`);
    return result.outcome === 403;
  } finally {
    session.close();
  }
}

async function main(): Promise<number> {
  const cfg = loadConfig();
  const results: [string, boolean][] = [];

  console.log("== 1. streaming request (contoso-support) ==");
  const { token, sub } = await signIn(cfg, "contoso-support");
  const session = new Session(cfg, token, sub);
  await session.connect();
  try {
    results.push(["normal streaming request", await testStreaming(session)]);
    console.log("== 2. response cache (cacheable prompt x2) ==");
    results.push(["second identical prompt is a cache hit", await testCache(session)]);
  } finally {
    session.close();
  }

  console.log("== 3. quota enforcement (fabrikam) ==");
  results.push(["fabrikam is rejected with 429 once over quota", await testQuota(cfg)]);

  console.log("== 4. model allow-list (contoso-knowledge -> model-a) ==");
  results.push(["knowledge-bot is denied model-a with 403", await testPolicy(cfg)]);

  console.log("\n== summary ==");
  for (const [name, ok] of results) console.log(`  ${ok ? "PASS" : "FAIL"}  ${name}`);
  return results.every(([, ok]) => ok) ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
