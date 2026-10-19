#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";

import { AiGatewayStack } from "../lib/stack.ts";

const app = new cdk.App();
new AiGatewayStack(app, "AiGateway", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION ?? "us-east-1",
  },
});
app.synth();
