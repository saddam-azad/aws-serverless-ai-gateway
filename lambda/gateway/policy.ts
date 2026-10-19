/**
 * Passport control and the baggage scale: tenant rules, model routing and token quotas.
 */
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const TENANT_CACHE_SECONDS = 60;
const QUOTA_TTL_SECONDS = 2 * 24 * 3600;

// Tenant rules are free-form DynamoDB items (config/tenants.json), so they are
// intentionally loosely typed: the JSON shape is the contract.
export type TenantConfig = Record<string, any>;
export type Rules = Record<string, any>;

let client: DynamoDBDocumentClient | undefined;
const tenants = new Map<string, { at: number; item: TenantConfig | undefined }>();

export function table(): DynamoDBDocumentClient {
  client ??= DynamoDBDocumentClient.from(new DynamoDBClient({}));
  return client;
}

export async function tenantConfig(tenant: string): Promise<TenantConfig | undefined> {
  const hit = tenants.get(tenant);
  if (hit && hit.at > Date.now() / 1000) return hit.item;
  const res = await table().send(
    new GetCommand({ TableName: process.env.TABLE_NAME, Key: { pk: `TENANT#${tenant}` } }),
  );
  const item = res.Item as TenantConfig | undefined;
  tenants.set(tenant, { at: Date.now() / 1000 + TENANT_CACHE_SECONDS, item });
  return item;
}

/** Tenant defaults, narrowed by any per-app override. */
export function appPolicy(tenantCfg: TenantConfig, app: string): Rules {
  const rules: Rules = {};
  for (const [key, value] of Object.entries(tenantCfg)) {
    if (key !== "pk" && key !== "apps") rules[key] = value;
  }
  Object.assign(rules, (tenantCfg.apps as Record<string, Rules> | undefined)?.[app] ?? {});
  return rules;
}

export function pickModel(rules: Rules, requested?: string | null): string {
  return requested || rules.default_model;
}

export function authorized(rules: Rules, model: string): boolean {
  return ((rules.allowed_models as string[] | undefined) ?? []).includes(model);
}

export function fallbackChain(rules: Rules, model: string): string[] {
  const chain = [model];
  const fallbacks = ((rules.fallbacks as Record<string, string[]> | undefined) ?? {})[model] ?? [];
  for (const alt of fallbacks) {
    if (!chain.includes(alt) && authorized(rules, alt)) chain.push(alt);
  }
  return chain;
}

/** Input can be estimated up front (~4 chars per token); output only has a ceiling. */
export function estimateTokens(prompt: string, maxTokens: number): number {
  return Math.ceil(prompt.length / 4) + maxTokens;
}

export class Reservation {
  readonly key: string;
  readonly estimate: number;
  readonly used: number;

  constructor(key: string, estimate: number, used: number) {
    this.key = key;
    this.estimate = estimate;
    this.used = used;
  }
}

/** Atomically reserve tokens in this hour's bucket, or return null when over quota. */
export async function reserveQuota(
  tenant: string,
  estimate: number,
  limit: number,
  now: Date = new Date(),
): Promise<Reservation | null> {
  if (estimate > limit) return null;
  const key = `QUOTA#${tenant}#${quotaHour(now)}`;
  try {
    const resp = await table().send(
      new UpdateCommand({
        TableName: process.env.TABLE_NAME,
        Key: { pk: key },
        UpdateExpression: "ADD tokens_used :est SET expires_at = if_not_exists(expires_at, :exp)",
        ConditionExpression: "attribute_not_exists(tokens_used) OR tokens_used <= :ceiling",
        ExpressionAttributeValues: {
          ":est": estimate,
          ":ceiling": limit - estimate,
          ":exp": Math.floor(now.getTime() / 1000) + QUOTA_TTL_SECONDS,
        },
        ReturnValues: "UPDATED_NEW",
      }),
    );
    return new Reservation(key, estimate, Number(resp.Attributes?.tokens_used));
  } catch (err) {
    if (err instanceof Error && err.name === "ConditionalCheckFailedException") return null;
    throw err;
  }
}

export async function reconcile(reservation: Reservation, actualTokens: number): Promise<void> {
  const delta = actualTokens - reservation.estimate;
  if (!delta) return;
  await table().send(
    new UpdateCommand({
      TableName: process.env.TABLE_NAME,
      Key: { pk: reservation.key },
      UpdateExpression: "ADD tokens_used :d",
      ExpressionAttributeValues: { ":d": delta },
    }),
  );
}

function quotaHour(now: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}${pad(now.getUTCHours())}`;
}
