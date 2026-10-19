/**
 * Gate 3, the boarding gate: publish tokens to AppSync Events as they arrive.
 *
 * Tokens are batched (first token immediately, then every FLUSH_MS or FLUSH_CHARS)
 * so a 500-token answer is ~25 publishes instead of 500.
 *
 * AppSync Events delivery is best effort: an accepted event can occasionally not
 * reach a connected subscriber, and order is not guaranteed. So every chunk has a
 * sequence number, `done` carries the complete answer (the source of truth, which
 * repairs any lost chunk), and terminal events (`done`, `error`) are sent
 * DONE_COPIES times so one lost copy does not leave the client waiting.
 *
 * AppSync also throttles the publish API itself under load (HTTP 429
 * LimitExceededException). That is transient, so publishes retry with jittered
 * backoff. Because the stream is best effort, a publish that still fails after
 * retries is dropped and logged rather than turned into a 500: the answer was
 * already generated, and `done` on a later attempt is what repairs the client.
 */
import { Sha256 } from "@aws-crypto/sha256-js";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { HttpRequest } from "@smithy/protocol-http";
import { SignatureV4 } from "@smithy/signature-v4";

import * as telemetry from "./telemetry.ts";

const FLUSH_MS = Number(process.env.FLUSH_MS ?? "100");
const FLUSH_CHARS = Number(process.env.FLUSH_CHARS ?? "400");
const DONE_COPIES = Number(process.env.DONE_COPIES ?? "2");
const MAX_DONE_TEXT_CHARS = 100_000; // AppSync Events allows 240 KB per event

// AppSync Events throttles the publish API under load. Throttles and server
// errors are transient, so retry with jittered backoff; other 4xx fail fast.
const PUBLISH_ATTEMPTS = Number(process.env.PUBLISH_ATTEMPTS ?? "5");
const PUBLISH_BACKOFF_BASE_S = Number(process.env.PUBLISH_BACKOFF_BASE_S ?? "0.1");
const PUBLISH_BACKOFF_CAP_S = Number(process.env.PUBLISH_BACKOFF_CAP_S ?? "2.0");
const RETRYABLE_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

export class PublishError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PublishError";
  }
}

export type Post = (url: string, host: string, body: string) => Promise<[number, string]>;

let signer: SignatureV4 | undefined;

function getSigner(): SignatureV4 {
  if (!signer) {
    const region = process.env.AWS_REGION ?? "us-east-1";
    signer = new SignatureV4({
      credentials: defaultProvider(),
      region,
      service: "appsync",
      sha256: Sha256,
    });
  }
  return signer;
}

/** Exponential backoff with full jitter, so retries do not sync up. */
export function backoff(attempt: number): number {
  return Math.random() * Math.min(PUBLISH_BACKOFF_CAP_S, PUBLISH_BACKOFF_BASE_S * 2 ** (attempt - 1));
}

const sleepSeconds = (seconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, seconds * 1000));

async function post(url: string, host: string, body: string): Promise<[number, string]> {
  const signed = await getSigner().sign(
    new HttpRequest({
      method: "POST",
      protocol: "https:",
      hostname: host,
      path: "/event",
      headers: { "content-type": "application/json", host },
      body,
    }),
  );
  const res = await fetch(url, {
    method: "POST",
    headers: signed.headers as Record<string, string>,
    body,
  });
  return [res.status, await res.text()];
}

export interface PublishOptions {
  attempts?: number;
  sleep?: (seconds: number) => Promise<void>;
  post?: Post;
}

/** Publish events, retrying with jittered backoff on transient failures. */
export async function publishEvents(
  channel: string,
  events: unknown[],
  options: PublishOptions = {},
): Promise<void> {
  const attempts = options.attempts ?? PUBLISH_ATTEMPTS;
  const nap = options.sleep ?? sleepSeconds;
  const send = options.post ?? post;
  const host = process.env.EVENTS_HTTP_DNS as string;
  const url = `https://${host}/event`;
  const body = JSON.stringify({ channel, events: events.map((event) => JSON.stringify(event)) });
  let reason = "publish failed";
  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) await nap(backoff(attempt));
    let status: number;
    let data: string;
    try {
      [status, data] = await send(url, host, body);
    } catch (err) {
      reason = `AppSync publish request failed: ${String(err)}`;
      continue;
    }
    if (status === 200) {
      const failed = (JSON.parse(data || "{}") as { failed?: unknown[] }).failed ?? [];
      if (!failed.length) return;
      // Per-event failures (e.g. LimitExceededException) are also worth a retry.
      reason = `AppSync rejected events: ${JSON.stringify(failed)}`;
      continue;
    }
    if (!RETRYABLE_STATUS.has(status)) {
      throw new PublishError(`AppSync publish returned ${status}: ${data.slice(0, 200)}`);
    }
    reason = `AppSync publish returned ${status}: ${data.slice(0, 200)}`;
  }
  throw new PublishError(reason);
}

export interface TokenStreamOptions {
  publish?: (channel: string, events: unknown[]) => Promise<void>;
  clock?: () => number;
}

export class TokenStream {
  readonly channel: string;
  readonly requestId: string;
  private readonly publish: (channel: string, events: unknown[]) => Promise<void>;
  private readonly clock: () => number;
  private readonly started: number;
  private firstTextAt: number | null = null;
  private seq = 0;
  private chars = 0;
  private readonly parts: string[] = [];
  private buffer: string[] = [];
  private buffered = 0;
  private lastFlush: number;
  publishFailures = 0;

  constructor(channel: string, requestId: string, options: TokenStreamOptions = {}) {
    this.channel = channel;
    this.requestId = requestId;
    this.publish = options.publish ?? ((ch, ev) => publishEvents(ch, ev));
    this.clock = options.clock ?? (() => performance.now() / 1000);
    this.started = this.clock();
    this.lastFlush = this.started;
  }

  get fullText(): string {
    return this.parts.join("");
  }

  async text(chunk: string): Promise<void> {
    if (!chunk) return;
    const first = this.firstTextAt === null;
    if (first) this.firstTextAt = this.clock();
    this.parts.push(chunk);
    this.buffer.push(chunk);
    this.buffered += chunk.length;
    this.chars += chunk.length;
    if (first || this.buffered >= FLUSH_CHARS || (this.clock() - this.lastFlush) * 1000 >= FLUSH_MS) {
      await this.flush();
    }
  }

  async flush(): Promise<void> {
    if (!this.buffer.length) return;
    this.seq += 1;
    const text = this.buffer.join("");
    this.buffer = [];
    this.buffered = 0;
    this.lastFlush = this.clock();
    await this.send({ type: "token", seq: this.seq, text });
  }

  async done(summary: Record<string, unknown>): Promise<void> {
    await this.flush();
    const event: Record<string, unknown> = {
      type: "done",
      chunks: this.seq,
      chars: this.chars,
      server_ms: this.elapsedMs(),
      ...summary,
    };
    if (this.chars <= MAX_DONE_TEXT_CHARS) event.text = this.fullText;
    for (let copy = 1; copy <= DONE_COPIES; copy++) {
      await this.send({ ...event, copy });
    }
  }

  async error(status: number, message: string): Promise<void> {
    // Terminal events are sent DONE_COPIES times: a lost 429 would leave the client waiting.
    await this.flush();
    for (let copy = 1; copy <= DONE_COPIES; copy++) {
      await this.send({ type: "error", status, message, copy });
    }
  }

  elapsedMs(): number {
    return Math.round((this.clock() - this.started) * 1000);
  }

  ttftMs(): number | null {
    if (this.firstTextAt === null) return null;
    return Math.round((this.firstTextAt - this.started) * 1000);
  }

  private async send(event: Record<string, unknown>): Promise<void> {
    // The stream is best effort: a publish that fails even after retries is
    // dropped and logged, never raised. The generation already happened, and a
    // later `done` copy is what lets the client repair a lost chunk.
    try {
      await this.publish(this.channel, [{ request_id: this.requestId, ...event }]);
    } catch {
      this.publishFailures += 1;
      telemetry.logException("publish_failed", this.requestId);
      telemetry.emitMetrics({ PublishFailures: 1 });
    }
  }
}
