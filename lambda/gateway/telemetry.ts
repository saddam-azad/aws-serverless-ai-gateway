/**
 * Gates 8-10: structured logs keyed by request_id, CloudWatch metrics, and usage records.
 */
import { FirehoseClient, PutRecordCommand } from "@aws-sdk/client-firehose";

export const NAMESPACE = process.env.METRICS_NAMESPACE ?? "AIGateway";

const UNITS: Record<string, string> = {
  Latency: "Milliseconds",
  TTFT: "Milliseconds",
  TokensPerSecond: "Count/Second",
  QuotaUtilization: "Percent",
};

let firehose: FirehoseClient | undefined;

export function log(stage: string, requestId: string | null, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ level: "INFO", stage, request_id: requestId, ...fields }));
}

export function logException(stage: string, requestId: string | null): void {
  console.log(
    JSON.stringify({ level: "ERROR", stage, request_id: requestId, trace: new Error().stack ?? "" }),
  );
}

/** CloudWatch Embedded Metric Format: metrics from a log line, no API calls on the hot path. */
export function emitMetrics(
  values: Record<string, number | null | undefined>,
  dimensions?: Record<string, string>,
): void {
  const entries = Object.entries(values).filter(
    (entry): entry is [string, number] => entry[1] !== null && entry[1] !== undefined,
  );
  if (!entries.length) return;
  const dims: Record<string, string> = { Service: "ai-gateway", ...dimensions };
  const body: Record<string, unknown> = {
    _aws: {
      Timestamp: Date.now(),
      CloudWatchMetrics: [
        {
          Namespace: NAMESPACE,
          Dimensions: [Object.keys(dims)],
          Metrics: entries.map(([name]) => ({ Name: name, Unit: UNITS[name] ?? "Count" })),
        },
      ],
    },
    ...dims,
  };
  for (const [name, value] of entries) body[name] = value;
  console.log(JSON.stringify(body));
}

export async function shipUsageRecord(record: Record<string, unknown>): Promise<void> {
  try {
    firehose ??= new FirehoseClient({});
    await firehose.send(
      new PutRecordCommand({
        DeliveryStreamName: process.env.USAGE_STREAM,
        Record: { Data: Buffer.from(`${JSON.stringify(record)}\n`) },
      }),
    );
  } catch {
    // Losing one usage record must never fail the passenger's request.
    logException("usage_record_failed", (record.request_id as string | undefined) ?? null);
  }
}
