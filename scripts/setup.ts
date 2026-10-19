/**
 * After `cdk deploy`: seed tenant rules, create test users, and write local config.
 *
 * Writes loadtest/config.json and client/config.js (both gitignored, they hold the
 * generated test password).
 */
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import * as path from "node:path";

import {
  AdminCreateUserCommand,
  AdminSetUserPasswordCommand,
  AdminUpdateUserAttributesCommand,
  CognitoIdentityProviderClient,
} from "@aws-sdk/client-cognito-identity-provider";
import { CloudFormationClient, DescribeStacksCommand } from "@aws-sdk/client-cloudformation";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

const ROOT = path.resolve(import.meta.dirname, "..");
const STACK = "AiGateway";

const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? "us-east-1";

interface TenantUser {
  username: string;
  tenant: string;
  app: string;
}

interface TenantConfig {
  tenants: Record<string, Record<string, unknown>>;
  users: TenantUser[];
}

async function stackOutputs(): Promise<Record<string, string>> {
  const cf = new CloudFormationClient({ region });
  const res = await cf.send(new DescribeStacksCommand({ StackName: STACK }));
  const outputs = res.Stacks?.[0]?.Outputs ?? [];
  return Object.fromEntries(outputs.map((o) => [o.OutputKey as string, o.OutputValue as string]));
}

/** Reuse the password already written, so re-running setup does not invalidate it. */
function testPassword(): string {
  const existing = path.join(ROOT, "loadtest", "config.json");
  if (existsSync(existing)) return (JSON.parse(readFileSync(existing, "utf8")) as { password: string }).password;
  return `Gw1!${randomBytes(18).toString("base64url")}`;
}

async function main(): Promise<void> {
  const out = await stackOutputs();
  const config = JSON.parse(readFileSync(path.join(ROOT, "config", "tenants.json"), "utf8")) as TenantConfig;

  const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
  for (const [name, rules] of Object.entries(config.tenants)) {
    await doc.send(new PutCommand({ TableName: out.StateTableName, Item: { pk: `TENANT#${name}`, ...rules } }));
    console.log(`seeded tenant ${name}`);
  }

  const cognito = new CognitoIdentityProviderClient({ region });
  const password = testPassword();
  for (const user of config.users) {
    const userAttributes = [
      { Name: "custom:tenant", Value: user.tenant },
      { Name: "custom:app", Value: user.app },
    ];
    try {
      await cognito.send(
        new AdminCreateUserCommand({
          UserPoolId: out.UserPoolId,
          Username: user.username,
          UserAttributes: userAttributes,
          MessageAction: "SUPPRESS",
        }),
      );
    } catch (err) {
      if ((err as Error).name !== "UsernameExistsException") throw err;
      await cognito.send(
        new AdminUpdateUserAttributesCommand({
          UserPoolId: out.UserPoolId,
          Username: user.username,
          UserAttributes: userAttributes,
        }),
      );
    }
    await cognito.send(
      new AdminSetUserPasswordCommand({
        UserPoolId: out.UserPoolId,
        Username: user.username,
        Password: password,
        Permanent: true,
      }),
    );
    console.log(`user ${user.username} -> ${user.tenant}/${user.app}`);
  }

  const local = {
    region,
    httpDns: out.EventsHttpDns,
    realtimeDns: out.EventsRealtimeDns,
    clientId: out.UserPoolClientId,
    password,
    users: config.users,
  };
  writeFileSync(path.join(ROOT, "loadtest", "config.json"), `${JSON.stringify(local, null, 2)}\n`);
  writeFileSync(path.join(ROOT, "client", "config.js"), `window.GATEWAY_CONFIG = ${JSON.stringify(local, null, 2)};\n`);
  console.log("\nwrote loadtest/config.json and client/config.js");
  console.log(`dashboard: ${out.DashboardUrl}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
