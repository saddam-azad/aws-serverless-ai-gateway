/**
 * Gate 7, the flights: Amazon Bedrock (or a mock model), with retries and fallback.
 *
 * MODEL_MAP maps the aliases tenants see (model-a, mock-a, ...) to a Bedrock model
 * or inference profile ID, or to "mock:flaky" / "mock:steady". Mock models stream
 * fake tokens so load tests measure the gateway without paying for inference.
 */
import {
  BedrockRuntimeClient,
  type ConverseStreamCommandOutput,
  ConverseStreamCommand,
} from "@aws-sdk/client-bedrock-runtime";
import { NodeHttpHandler } from "@smithy/node-http-handler";

const MAX_RETRIES = 2;
const BACKOFF_BASE_S = 0.2;
const BACKOFF_CAP_S = 2.0;
const RETRYABLE = new Set([
  "ThrottlingException",
  "ServiceUnavailableException",
  "ModelNotReadyException",
  "InternalServerException",
  "ModelStreamErrorException",
]);
const STREAM_ERRORS = [
  "throttlingException",
  "serviceUnavailableException",
  "internalServerException",
  "modelStreamErrorException",
];

const MOCK_THROTTLE_RATE = Number(process.env.MOCK_THROTTLE_RATE ?? "0.05");
const MOCK_TTFT_MS = Number(process.env.MOCK_TTFT_MS ?? "250");
const MOCK_TOKEN_MS = Number(process.env.MOCK_TOKEN_MS ?? "8");
const MOCK_WORDS = (
  "The gateway checked the passport, weighed the baggage, found a seat on the flight " +
  "and streamed every word back to the gate while the control tower kept the records."
).split(" ");

export interface Usage {
  input_tokens: number;
  output_tokens: number;
}

export type StreamPiece = ["text", string] | ["usage", Usage];
export type StreamFn = (
  alias: string,
  prompt: string,
  system: string,
  maxTokens: number,
) => AsyncGenerator<StreamPiece>;

export class ModelThrottled extends Error {}

export class AllModelsBusy extends Error {
  readonly retries: number;
  readonly throttles: number;

  constructor(retries: number, throttles: number) {
    super("every model in the chain is throttled");
    this.name = "AllModelsBusy";
    this.retries = retries;
    this.throttles = throttles;
  }
}

/** A model failed after tokens were already sent, so it is too late to switch. */
export class StreamInterrupted extends Error {
  readonly model: string;
  readonly retries: number;
  readonly throttles: number;

  constructor(model: string, retries: number, throttles: number) {
    super(`${model} failed mid-stream`);
    this.name = "StreamInterrupted";
    this.model = model;
    this.retries = retries;
    this.throttles = throttles;
  }
}

export interface Result {
  model: string;
  input_tokens: number;
  output_tokens: number;
  fallback: boolean;
  retries: number;
  throttles: number;
}

export const sleep = (seconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, seconds * 1000));

/** Exponential backoff with full jitter. */
function backoff(attempt: number): number {
  return Math.random() * Math.min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2 ** attempt);
}

export async function generate(
  chain: string[],
  prompt: string,
  system: string,
  maxTokens: number,
  onText: (chunk: string) => void | Promise<void>,
  options: { streamFn?: StreamFn; sleep?: (seconds: number) => Promise<void> } = {},
): Promise<Result> {
  const streamFn = options.streamFn ?? streamModel;
  const nap = options.sleep ?? sleep;
  let retries = 0;
  let throttles = 0;
  for (let position = 0; position < chain.length; position++) {
    const alias = chain[position];
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt) {
        retries += 1;
        await nap(backoff(attempt));
      }
      let sent = false;
      let usage: Usage = { input_tokens: 0, output_tokens: 0 };
      try {
        for await (const [kind, value] of streamFn(alias, prompt, system, maxTokens)) {
          if (kind === "text") {
            sent = true;
            await onText(value);
          } else {
            usage = value;
          }
        }
        return {
          model: alias,
          input_tokens: Math.trunc(usage.input_tokens ?? 0),
          output_tokens: Math.trunc(usage.output_tokens ?? 0),
          fallback: position > 0,
          retries,
          throttles,
        };
      } catch (err) {
        if (err instanceof ModelThrottled) {
          throttles += 1;
          if (sent) throw new StreamInterrupted(alias, retries, throttles);
          continue;
        }
        throw err;
      }
    }
  }
  throw new AllModelsBusy(retries, throttles);
}

/** Yield ("text", chunk) pieces, then one ("usage", {...}). Throw ModelThrottled if retryable. */
export async function* streamModel(
  alias: string,
  prompt: string,
  system: string,
  maxTokens: number,
): AsyncGenerator<StreamPiece> {
  const target = (JSON.parse(process.env.MODEL_MAP ?? "{}") as Record<string, string>)[alias];
  if (target === undefined) throw new Error(`No model is mapped to alias ${alias}`);
  if (target.startsWith("mock:")) {
    yield* mockStream(target.split(":", 2)[1], prompt, maxTokens);
    return;
  }
  yield* bedrockStream(target, prompt, system, maxTokens);
}

async function* mockStream(kind: string, prompt: string, maxTokens: number): AsyncGenerator<StreamPiece> {
  // Real throttles come back fast, before any tokens.
  if (kind === "flaky" && Math.random() < MOCK_THROTTLE_RATE) throw new ModelThrottled("mock ThrottlingException");
  await sleep((MOCK_TTFT_MS * (0.7 + Math.random() * 0.6)) / 1000);
  for (let i = 0; i < maxTokens; i++) {
    yield ["text", `${MOCK_WORDS[i % MOCK_WORDS.length]} `];
    await sleep(MOCK_TOKEN_MS / 1000);
  }
  yield ["usage", { input_tokens: Math.ceil(prompt.length / 4), output_tokens: maxTokens }];
}

let bedrockClient: BedrockRuntimeClient | undefined;

function bedrock(): BedrockRuntimeClient {
  // Retries are ours, so they show up in the metrics.
  bedrockClient ??= new BedrockRuntimeClient({
    maxAttempts: 1,
    requestHandler: new NodeHttpHandler({ requestTimeout: 120_000 }),
  });
  return bedrockClient;
}

function errorCode(err: unknown): string {
  const e = err as { name?: string; Code?: string };
  return e?.name ?? e?.Code ?? "Unknown";
}

async function* bedrockStream(
  modelId: string,
  prompt: string,
  system: string,
  maxTokens: number,
): AsyncGenerator<StreamPiece> {
  let response: ConverseStreamCommandOutput;
  try {
    response = await bedrock().send(
      new ConverseStreamCommand({
        modelId,
        system: [{ text: system }],
        messages: [{ role: "user", content: [{ text: prompt }] }],
        inferenceConfig: { maxTokens },
      }),
    );
  } catch (err) {
    if (RETRYABLE.has(errorCode(err))) throw new ModelThrottled(errorCode(err));
    throw err;
  }

  let usage: Usage = { input_tokens: 0, output_tokens: 0 };
  try {
    for await (const event of response.stream ?? []) {
      if (event.contentBlockDelta) {
        const text = event.contentBlockDelta.delta?.text;
        if (text) yield ["text", text];
      } else if (event.metadata) {
        const u = event.metadata.usage;
        usage = { input_tokens: u?.inputTokens ?? 0, output_tokens: u?.outputTokens ?? 0 };
      } else {
        for (const name of STREAM_ERRORS) {
          if (name in event) throw new ModelThrottled(name);
        }
      }
    }
  } catch (err) {
    if (RETRYABLE.has(errorCode(err))) throw new ModelThrottled(errorCode(err));
    throw err;
  }
  yield ["usage", usage];
}
