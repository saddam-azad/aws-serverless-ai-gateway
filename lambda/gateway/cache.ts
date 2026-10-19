/**
 * Gate 6, the express lane: a response cache that never crosses tenants.
 */
import { createHash } from "node:crypto";

import { GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

import { table } from "./policy.ts";

const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS ?? "3600");

export function cacheKey(
  tenant: string,
  app: string,
  model: string,
  systemVersion: string,
  prompt: string,
): string {
  // Everything that can change the right answer is part of the key.
  const material = JSON.stringify({
    app,
    model,
    prompt: prompt.trim().split(/\s+/).filter(Boolean).join(" ").toLowerCase(),
    system: systemVersion,
    tenant,
  });
  return `CACHE#${createHash("sha256").update(material).digest("hex")}`;
}

export async function lookup(key: string): Promise<string | null> {
  const res = await table().send(new GetCommand({ TableName: process.env.TABLE_NAME, Key: { pk: key } }));
  const item = res.Item;
  if (item && Number(item.expires_at) > Date.now() / 1000) return item.text as string;
  return null;
}

export async function store(key: string, text: string, model: string): Promise<void> {
  await table().send(
    new PutCommand({
      TableName: process.env.TABLE_NAME,
      Item: {
        pk: key,
        text,
        model,
        expires_at: Math.floor(Date.now() / 1000) + CACHE_TTL_SECONDS,
      },
    }),
  );
}
