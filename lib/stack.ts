/**
 * AI International: the serverless AI gateway from Part 1, as one CDK stack.
 *
 *   WAF -> Cognito -> AppSync Events -> Lambda -> Bedrock
 *                                         |-> DynamoDB (quotas, cache, tenants)
 *                                         |-> Firehose -> S3 -> Glue -> Athena
 *                                         '-> CloudWatch (EMF metrics, dashboard, alarm)
 */
import { readFileSync, readdirSync } from "node:fs";
import * as path from "node:path";

import {
  Aws,
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  type StackProps,
  aws_appsync as appsync,
  aws_athena as athena,
  aws_cloudwatch as cw,
  aws_cognito as cognito,
  aws_dynamodb as dynamodb,
  aws_glue as glue,
  aws_iam as iam,
  aws_kinesisfirehose as firehose,
  aws_lambda as lambda_,
  aws_lambda_nodejs as nodejs,
  aws_logs as logs,
  aws_s3 as s3,
  aws_wafv2 as wafv2,
} from "aws-cdk-lib";
import { Construct } from "constructs";

const ROOT = path.resolve(import.meta.dirname, "..");
const METRICS_NAMESPACE = "AIGateway";
const GLUE_DATABASE = "ai_gateway";
const USAGE_TABLE = "ai_usage";

// aws-cdk-lib predates the Node.js 26 runtime constant, so name it directly.
const NODEJS_26_X = new lambda_.Runtime("nodejs26.x", lambda_.RuntimeFamily.NODEJS);

export class AiGatewayStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const enableWaf = String(this.node.tryGetContext("waf")).toLowerCase() !== "false";
    const wafRateLimit = Number(this.node.tryGetContext("wafRateLimitPerMinute") ?? 10000);
    const mockThrottleRate = String(this.node.tryGetContext("mockThrottleRate") ?? "0.05");
    const modelMap = JSON.parse(
      readFileSync(path.join(ROOT, "config", "models.json"), "utf8"),
    ) as Record<string, string>;

    // ---- Baggage scale, express lane and tenant rules: one DynamoDB table ----
    const state = new dynamodb.Table(this, "State", {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "expires_at",
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // ---- Gate 2: passport control ----
    const userPool = new cognito.UserPool(this, "Passengers", {
      selfSignUpEnabled: false,
      signInAliases: { username: true },
      customAttributes: {
        tenant: new cognito.StringAttribute({ minLen: 1, maxLen: 64, mutable: true }),
        app: new cognito.StringAttribute({ minLen: 1, maxLen: 64, mutable: true }),
      },
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const userPoolClient = userPool.addClient("GatewayClient", {
      authFlows: { userPassword: true, userSrp: true },
      generateSecret: false,
      idTokenValidity: Duration.hours(1),
      accessTokenValidity: Duration.hours(1),
      readAttributes: new cognito.ClientAttributes()
        .withStandardAttributes({ email: true })
        .withCustomAttributes("tenant", "app"),
      // Users must never be able to rewrite their own tenant or app.
      writeAttributes: new cognito.ClientAttributes().withStandardAttributes({ email: true }),
    });

    // ---- Gate 3: the boarding gate ----
    const userPoolMode = appsync.AppSyncAuthorizationType.USER_POOL;
    const iamMode = appsync.AppSyncAuthorizationType.IAM;
    const api = new appsync.EventApi(this, "BoardingGate", {
      apiName: "ai-gateway",
      authorizationConfig: {
        authProviders: [
          { authorizationType: userPoolMode, cognitoConfig: { userPool } },
          { authorizationType: iamMode },
        ],
        connectionAuthModeTypes: [userPoolMode, iamMode],
        defaultPublishAuthModeTypes: [userPoolMode],
        defaultSubscribeAuthModeTypes: [userPoolMode],
      },
      logConfig: {
        fieldLogLevel: appsync.AppSyncFieldLogLevel.ERROR,
        retention: logs.RetentionDays.ONE_WEEK,
      },
    });

    // ---- Gate 8: the accounting office and records archive ----
    const usageBucket = new s3.Bucket(this, "UsageRecords", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      lifecycleRules: [{ expiration: Duration.days(30) }],
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    const firehoseRole = new iam.Role(this, "FirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
    });
    usageBucket.grantReadWrite(firehoseRole);
    const usageStream = new firehose.CfnDeliveryStream(this, "UsageStream", {
      deliveryStreamType: "DirectPut",
      extendedS3DestinationConfiguration: {
        bucketArn: usageBucket.bucketArn,
        roleArn: firehoseRole.roleArn,
        prefix: "usage/day=!{timestamp:yyyy-MM-dd}/",
        errorOutputPrefix: "errors/!{firehose:error-output-type}/day=!{timestamp:yyyy-MM-dd}/",
        bufferingHints: { intervalInSeconds: 60, sizeInMBs: 5 },
        compressionFormat: "GZIP",
      },
    });
    usageStream.node.addDependency(firehoseRole);

    // ---- Gate 4: the gate agent ----
    const gateAgentLogs = new logs.LogGroup(this, "GateAgentLogs", {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const gateAgent = new nodejs.NodejsFunction(this, "GateAgent", {
      entry: path.join(ROOT, "lambda", "gateway", "handler.ts"),
      handler: "handler",
      runtime: NODEJS_26_X,
      architecture: lambda_.Architecture.ARM_64,
      memorySize: 256,
      timeout: Duration.seconds(120),
      logGroup: gateAgentLogs,
      bundling: {
        minify: true,
        sourceMap: false,
        target: "es2022",
        // Bundle the AWS SDK (including @smithy/*) so the function never depends
        // on whatever the Node runtime happens to ship.
        bundleAwsSDK: true,
      },
      environment: {
        TABLE_NAME: state.tableName,
        USAGE_STREAM: usageStream.ref,
        EVENTS_HTTP_DNS: api.httpDns,
        MODEL_MAP: JSON.stringify(modelMap),
        METRICS_NAMESPACE,
        MOCK_THROTTLE_RATE: mockThrottleRate,
      },
    });
    state.grantReadWriteData(gateAgent);
    gateAgent.addToRolePolicy(
      new iam.PolicyStatement({ actions: ["firehose:PutRecord"], resources: [usageStream.attrArn] }),
    );
    gateAgent.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["appsync:EventConnect", "appsync:EventPublish"],
        resources: [api.apiArn, `${api.apiArn}/*`],
      }),
    );
    // Gate 7: the flights. Cross-Region inference profiles route to several Regions.
    gateAgent.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
        resources: [
          "arn:aws:bedrock:*::foundation-model/*",
          `arn:aws:bedrock:*:${Aws.ACCOUNT_ID}:inference-profile/*`,
        ],
      }),
    );

    // Clients publish requests here. Nobody subscribes; Lambda handles each one async.
    const gateAgentSource = api.addLambdaDataSource("GateAgentSource", gateAgent);
    api.addChannelNamespace("requests", {
      authorizationConfig: { publishAuthModeTypes: [userPoolMode], subscribeAuthModeTypes: [iamMode] },
      publishHandlerConfig: {
        dataSource: gateAgentSource,
        direct: true,
        lambdaInvokeType: appsync.LambdaInvokeType.EVENT,
      },
    });
    // Lambda streams tokens here; each user may only subscribe to their own channel.
    api.addChannelNamespace("responses", {
      authorizationConfig: { publishAuthModeTypes: [iamMode], subscribeAuthModeTypes: [userPoolMode] },
      code: appsync.Code.fromAsset(path.join(ROOT, "appsync", "responses.js")),
    });

    // ---- Gate 1: perimeter security ----
    if (enableWaf) {
      this.perimeter(api, wafRateLimit);
    }

    // ---- Gate 8: filing system and auditors ----
    this.usageCatalog(usageBucket);

    // ---- Gates 9 and 10: tracking number and control tower ----
    const dashboardName = this.controlTower(gateAgent, gateAgentLogs);

    new CfnOutput(this, "EventsHttpDns", { value: api.httpDns });
    new CfnOutput(this, "EventsRealtimeDns", { value: api.realtimeDns });
    new CfnOutput(this, "UserPoolId", { value: userPool.userPoolId });
    new CfnOutput(this, "UserPoolClientId", { value: userPoolClient.userPoolClientId });
    new CfnOutput(this, "StateTableName", { value: state.tableName });
    new CfnOutput(this, "GateAgentFunction", { value: gateAgent.functionName });
    new CfnOutput(this, "AthenaWorkgroup", { value: "ai-gateway" });
    new CfnOutput(this, "DashboardUrl", {
      value: `https://${Aws.REGION}.console.aws.amazon.com/cloudwatch/home?region=${Aws.REGION}#dashboards/dashboard/${dashboardName}`,
    });
  }

  private perimeter(api: appsync.IEventApi, rateLimit: number): void {
    const visibility = (name: string): wafv2.CfnWebACL.VisibilityConfigProperty => ({
      cloudWatchMetricsEnabled: true,
      metricName: name,
      sampledRequestsEnabled: true,
    });

    const managed = (name: string, priority: number): wafv2.CfnWebACL.RuleProperty => ({
      name,
      priority,
      overrideAction: { none: {} },
      statement: {
        managedRuleGroupStatement: { vendorName: "AWS", name },
      },
      visibilityConfig: visibility(name),
    });

    const acl = new wafv2.CfnWebACL(this, "Perimeter", {
      scope: "REGIONAL",
      defaultAction: { allow: {} },
      visibilityConfig: visibility("ai-gateway-perimeter"),
      rules: [
        managed("AWSManagedRulesAmazonIpReputationList", 0),
        managed("AWSManagedRulesKnownBadInputsRuleSet", 1),
        {
          name: "RateLimitPerIp",
          priority: 2,
          action: { block: {} },
          statement: {
            rateBasedStatement: { limit: rateLimit, aggregateKeyType: "IP", evaluationWindowSec: 60 },
          },
          visibilityConfig: visibility("RateLimitPerIp"),
        },
      ],
    });
    new wafv2.CfnWebACLAssociation(this, "PerimeterAssociation", {
      resourceArn: api.apiArn,
      webAclArn: acl.attrArn,
    });
  }

  private usageCatalog(usageBucket: s3.Bucket): void {
    const columns: [string, string][] = [
      ["request_id", "string"],
      ["tenant", "string"],
      ["app", "string"],
      ["user_id", "string"],
      ["requested_model", "string"],
      ["model", "string"],
      ["status", "int"],
      ["input_tokens", "int"],
      ["output_tokens", "int"],
      ["ttft_ms", "int"],
      ["latency_ms", "int"],
      ["cache", "string"],
      ["fallback", "boolean"],
      ["retries", "int"],
      ["throttles", "int"],
      ["error", "string"],
      ["timestamp", "string"],
    ];
    const database = new glue.CfnDatabase(this, "UsageDatabase", {
      catalogId: Aws.ACCOUNT_ID,
      databaseInput: { name: GLUE_DATABASE },
    });
    const table = new glue.CfnTable(this, "UsageTable", {
      catalogId: Aws.ACCOUNT_ID,
      databaseName: GLUE_DATABASE,
      tableInput: {
        name: USAGE_TABLE,
        tableType: "EXTERNAL_TABLE",
        // Partition projection: new days are queryable immediately, no crawler runs.
        parameters: {
          classification: "json",
          "projection.enabled": "true",
          "projection.day.type": "date",
          "projection.day.format": "yyyy-MM-dd",
          "projection.day.range": "2026-01-01,NOW",
          "projection.day.interval": "1",
          "projection.day.interval.unit": "DAYS",
          "storage.location.template": `s3://${usageBucket.bucketName}/usage/day=\${day}/`,
        },
        partitionKeys: [{ name: "day", type: "string" }],
        storageDescriptor: {
          location: `s3://${usageBucket.bucketName}/usage/`,
          inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
          outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
          serdeInfo: {
            serializationLibrary: "org.openx.data.jsonserde.JsonSerDe",
            parameters: { "ignore.malformed.json": "true" },
          },
          columns: columns.map(([name, type]) => ({ name, type })),
        },
      },
    });
    table.addResourceDependency(database);

    const results = new s3.Bucket(this, "AthenaResults", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      lifecycleRules: [{ expiration: Duration.days(7) }],
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    const workgroup = new athena.CfnWorkGroup(this, "Auditors", {
      name: "ai-gateway",
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        bytesScannedCutoffPerQuery: 1_000_000_000,
        resultConfiguration: { outputLocation: `s3://${results.bucketName}/` },
      },
    });
    const queriesDir = path.join(ROOT, "queries");
    for (const file of readdirSync(queriesDir).filter((f) => f.endsWith(".sql")).sort()) {
      const stem = path.basename(file, ".sql");
      const query = new athena.CfnNamedQuery(this, `Query-${stem}`, {
        name: stem.replace(/_/g, " "),
        database: GLUE_DATABASE,
        workGroup: workgroup.name,
        queryString: readFileSync(path.join(queriesDir, file), "utf8"),
      });
      query.addResourceDependency(workgroup);
    }
  }

  private controlTower(gateAgent: lambda_.IFunction, gateAgentLogs: logs.LogGroup): string {
    const m = (name: string, stat = "Sum", label?: string): cw.Metric =>
      new cw.Metric({
        namespace: METRICS_NAMESPACE,
        metricName: name,
        dimensionsMap: { Service: "ai-gateway" },
        statistic: stat,
        period: Duration.minutes(1),
        label: label ?? `${name} ${stat}`,
      });

    const rate = (label: string, numerator: cw.Metric, denominator: cw.Metric): cw.MathExpression =>
      new cw.MathExpression({
        expression: "IF(d > 0, 100 * n / d, 0)",
        usingMetrics: { n: numerator, d: denominator },
        label,
        period: Duration.minutes(1),
      });

    const percentiles = (name: string): cw.Metric[] => [
      m(name, "p50", "p50"),
      m(name, "p95", "p95"),
      m(name, "p99", "p99"),
    ];

    const requests = m("Requests");
    const fallbackRate = rate("Fallback rate %", m("Fallbacks"), requests);
    const throttleRate = rate("Throttle rate %", m("Throttles"), requests);
    const cacheHitRate = new cw.MathExpression({
      expression: "IF(h + x > 0, 100 * h / (h + x), 0)",
      usingMetrics: { h: m("CacheHits"), x: m("CacheMisses") },
      label: "Cache hit rate %",
      period: Duration.minutes(1),
    });
    const quotaByTenant = new cw.MathExpression({
      expression: `SEARCH('{${METRICS_NAMESPACE},Service,Tenant} MetricName="QuotaUtilization"', 'Maximum', 60)`,
      usingMetrics: {},
      label: "",
      period: Duration.minutes(1),
    });

    const dashboardName = "ai-gateway-control-tower";
    const dashboard = new cw.Dashboard(this, "ControlTower", { dashboardName });
    dashboard.addWidgets(
      new cw.SingleValueWidget({
        title: "Last hour",
        metrics: [
          m("Requests", "Sum", "Requests"),
          m("Latency", "p95", "p95 latency"),
          m("TTFT", "p95", "p95 TTFT"),
          m("QuotaRejections", "Sum", "Quota 429s"),
          m("Errors", "Sum", "Errors"),
        ],
        period: Duration.hours(1),
        width: 24,
      }),
    );
    dashboard.addWidgets(
      new cw.GraphWidget({
        title: "Latency (ms): averages hide the users who are suffering",
        left: percentiles("Latency"),
        width: 12,
      }),
      new cw.GraphWidget({
        title: "Time to first token (ms): what users actually feel",
        left: percentiles("TTFT"),
        width: 12,
      }),
    );
    dashboard.addWidgets(
      new cw.GraphWidget({
        title: "Tokens / second (streaming speed)",
        left: [m("TokensPerSecond", "p50", "p50"), m("TokensPerSecond", "Average", "avg")],
        width: 8,
      }),
      new cw.GraphWidget({
        title: "Fallback rate %: the early warning signal",
        left: [fallbackRate],
        width: 8,
      }),
      new cw.GraphWidget({ title: "Throttle rate % (model capacity)", left: [throttleRate], width: 8 }),
    );
    dashboard.addWidgets(
      new cw.GraphWidget({ title: "Quota utilization % by tenant", left: [quotaByTenant], width: 8 }),
      new cw.GraphWidget({ title: "Cache hit rate %", left: [cacheHitRate], width: 8 }),
      new cw.GraphWidget({
        title: "Outcomes per minute",
        left: [m("Requests"), m("QuotaRejections"), m("Denied"), m("Errors")],
        width: 8,
      }),
    );
    dashboard.addWidgets(
      new cw.GraphWidget({
        title: "Tokens per minute",
        left: [m("InputTokens"), m("OutputTokens")],
        width: 12,
      }),
      new cw.GraphWidget({
        title: "Gate agent (Lambda) concurrency and throttles",
        left: [gateAgent.metric("ConcurrentExecutions", { statistic: "Maximum", period: Duration.minutes(1) })],
        right: [gateAgent.metricThrottles({ period: Duration.minutes(1) })],
        width: 12,
      }),
    );

    new cw.Alarm(this, "FallbackRateHigh", {
      alarmName: "ai-gateway-fallback-rate-high",
      alarmDescription: "Primary model is struggling: more than 10% of requests fell back.",
      metric: fallbackRate,
      threshold: 10,
      evaluationPeriods: 5,
      datapointsToAlarm: 3,
      comparisonOperator: cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cw.TreatMissingData.NOT_BREACHING,
    });

    // Gate 9: pull up any single request by its tracking number.
    new logs.QueryDefinition(this, "FindRequest", {
      queryDefinitionName: "ai-gateway/find-request-by-id",
      queryString: new logs.QueryString({
        fields: ["@timestamp", "stage", "status", "model", "latency_ms", "ttft_ms", "retries", "fallback", "error"],
        filterStatements: ['request_id = "PASTE-REQUEST-ID"'],
        sort: "@timestamp asc",
      }),
      logGroups: [gateAgentLogs],
    });
    new logs.QueryDefinition(this, "SlowestRequests", {
      queryDefinitionName: "ai-gateway/slowest-requests",
      queryString: new logs.QueryString({
        fields: [
          "@timestamp",
          "request_id",
          "tenant",
          "app",
          "model",
          "latency_ms",
          "ttft_ms",
          "input_tokens",
          "fallback",
          "retries",
        ],
        filterStatements: ['stage = "completed"'],
        sort: "latency_ms desc",
        limit: 20,
      }),
      logGroups: [gateAgentLogs],
    });
    return dashboardName;
  }
}
