import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import { NatGatewayStrategy } from "@pulumi/awsx/ec2";

const config = new pulumi.Config("zero-cache");
const namespace = config.require("namespace");
const upstreamDb = config.require("upstreamDb");
const cvrDb = config.require("cvrDb");
const changeDb = config.require("changeDb");
const jwtSecret = config.require("jwtSecret");
const awsAccessKeyId = config.require("awsAccessKeyId");
const awsSecretAccessKey = config.require("awsSecretAccessKey");
const schemaJson = config.require("schemaJson");
// const certificateArn = config.get("certificateArn"); // Uncomment if needed for HTTPS

const backupBucket = new aws.s3.Bucket(`${namespace}-data-bucket`, {
  forceDestroy: false,
});

// VPC Configuration
const vpc = new awsx.ec2.Vpc(`${namespace}-vpc`, {
  subnetStrategy: "Auto",
  numberOfAvailabilityZones: 2,
  cidrBlock: "10.0.0.0/16",
  subnetSpecs: [
    {
      name: "public",
      type: "Public",
      cidrMask: 20,
      cidrBlocks: ["10.0.0.0/20", "10.0.16.0/20"],
    },
    {
      name: "private",
      type: "Private",
      cidrMask: 20,
      cidrBlocks: ["10.0.32.0/20", "10.0.48.0/20"],
    },
  ],
  natGateways: {
    strategy: NatGatewayStrategy.Single,
  },
  enableDnsHostnames: true,
  enableDnsSupport: true,
});

const ns = new aws.servicediscovery.HttpNamespace(`${namespace}-namespace`, {
  name: namespace,
});

const cluster = new aws.ecs.Cluster(`${namespace}-cluster`, {
  settings: [
    {
      name: "containerInsights",
      value: "enhanced",
    },
  ],
  serviceConnectDefaults: {
    namespace: ns.arn,
  },
});

// Create security group for the load balancer
const lbSecurityGroup = new aws.ec2.SecurityGroup(`${namespace}-lb-sg`, {
  description: "HTTP/HTTPS access to the public facing load balancer",
  vpcId: vpc.vpc.id,
  ingress: [
    {
      description: "Allow HTTP traffic",
      protocol: "tcp",
      fromPort: 80,
      toPort: 80,
      cidrBlocks: ["0.0.0.0/0"],
    },
    {
      description: "Allow traffic to ZeroCache service",
      protocol: "tcp",
      fromPort: 4848,
      toPort: 4848,
      cidrBlocks: ["0.0.0.0/0"],
    },
    {
      description: "Allow traffic to ZeroCache Heartbeat",
      protocol: "tcp",
      fromPort: 4850,
      toPort: 4850,
      cidrBlocks: ["0.0.0.0/0"],
    },
    {
      description: "Allow HTTPS traffic",
      protocol: "tcp",
      fromPort: 443,
      toPort: 443,
      cidrBlocks: ["0.0.0.0/0"],
    },
  ],
  egress: [
    {
      description: "Allow all outbound traffic by default",
      protocol: "-1",
      fromPort: 0,
      toPort: 0,
      cidrBlocks: ["0.0.0.0/0"],
    },
  ],
});

// Create security group for internal service communication
const internalServicesSG = new aws.ec2.SecurityGroup(
  `${namespace}-internal-sg`,
  {
    description: "Security group for internal service communication",
    vpcId: vpc.vpc.id,
    ingress: [
      {
        protocol: "tcp",
        fromPort: 4848,
        toPort: 4848,
        cidrBlocks: ["10.0.0.0/16"],
      },
      {
        protocol: "tcp",
        fromPort: 4849,
        toPort: 4849,
        cidrBlocks: ["10.0.0.0/16"],
      },
    ],
  },
);

const loadbalancer = new awsx.lb.ApplicationLoadBalancer("loadbalancer", {
  securityGroups: [lbSecurityGroup.id, internalServicesSG.id],
  subnets: [vpc.subnets[0], vpc.subnets[2]],
  defaultTargetGroup: {
    port: 4848,
    protocol: "HTTP",
    healthCheck: {
      enabled: true,
      path: "/",
      protocol: "HTTP",
      port: "4850",
      healthyThreshold: 3,
      unhealthyThreshold: 2,
      timeout: 3,
      interval: 5,
      matcher: "200",
    },
    targetType: "ip",
    deregistrationDelay: 0,
    stickiness: {
      enabled: true,
      type: "lb_cookie",
      cookieDuration: 300,
    },
  },
});

// Add HTTPS listener if you have a certificate
// const httpsListener = new aws.lb.Listener("https", {
//   loadBalancerArn: loadbalancer.loadBalancer.arn,
//   port: 443,
//   protocol: "HTTPS",
//   certificateArn: process.env.CERTIFICATE_ARN,
//   defaultActions: [{
//     type: "forward",
//     targetGroupArn: loadbalancer.defaultTargetGroup.arn,
//   }],
// });

// Create CloudWatch Log Groups
const viewSyncerLogGroup = new aws.cloudwatch.LogGroup(`${namespace}-view-syncer`, {
  retentionInDays: 30,
});

const replicationManagerLogGroup = new aws.cloudwatch.LogGroup(`${namespace}-replication-manager`, {
  retentionInDays: 30,
});

// View Syncer Service
new awsx.ecs.FargateService("view-syncer", {
  cluster: cluster.arn,
  desiredCount: 1,
  assignPublicIp: true,
  enableExecuteCommand: true,
  healthCheckGracePeriodSeconds: 300,
  deploymentMaximumPercent: 120,
  deploymentMinimumHealthyPercent: 50,
  deploymentCircuitBreaker: {
    enable: true,
    rollback: true,
  },
  taskDefinitionArgs: {
    container: {
      name: "view-syncer-container",
      image: "rocicorp/zero:canary",
      cpu: 8192,
      memory: 16384,
      essential: true,
      portMappings: [
        {
          name: "view-syncer",
          containerPort: 4848,
          hostPort: 4848,
          appProtocol: "http",
          protocol: "tcp",
        },
      ],
      healthCheck: {
        command: ["CMD-SHELL", "curl -f http://localhost:4848/ || exit 1"],
        interval: 5,
        retries: 3,
        startPeriod: 300,
      },
      environment: [
        { name: "AWS_REGION", value: aws.config.region },
        { name: "ZERO_UPSTREAM_DB", value: upstreamDb },
        { name: "ZERO_CVR_DB", value: cvrDb },
        { name: "ZERO_CHANGE_DB", value: changeDb },
        { name: "ZERO_AUTH_SECRET", value: jwtSecret },
        { name: "AWS_ACCESS_KEY_ID", value: awsAccessKeyId },
        { name: "AWS_SECRET_ACCESS_KEY", value: awsSecretAccessKey },
        {
          name: "ZERO_CHANGE_STREAMER_URI",
          value: `ws://change-streamer.${namespace}:4849`,
        },
        { name: "ZERO_SCHEMA_JSON", value: schemaJson },
        { name: "ZERO_LOG_FORMAT", value: "json" },
        { name: "ZERO_REPLICA_FILE", value: "sync-replica.db" },
        { name: "ZERO_UPSTREAM_MAX_CONNS", value: "15" },
        { name: "ZERO_CVR_MAX_CONNS", value: "160" },
        {
          name: "ZERO_LITESTREAM_BACKUP_URL",
          value: pulumi.interpolate`s3://${backupBucket.id}/backup`,
        },
      ],
      linuxParameters: {
        initProcessEnabled: true,
      },
      stopTimeout: 120,
      logConfiguration: {
        logDriver: "awslogs",
        options: {
          mode: "non-blocking",
          "max-buffer-size": "25m",
          "awslogs-group": viewSyncerLogGroup.name,
          "awslogs-region": aws.config.region,
          "awslogs-stream-prefix": "ecs",
        },
      },
    },
  },
  serviceConnectConfiguration: {
    enabled: true,
    namespace,
    services: [
      {
        portName: "view-syncer",
        clientAlias: [
          {
            port: 4848,
            dnsName: `view-syncer.${namespace}`,
          },
        ],
      },
    ],
  },
});

// Replication Manager Service
new awsx.ecs.FargateService("replication-manager", {
  cluster: cluster.arn,
  desiredCount: 1,
  assignPublicIp: true,
  enableExecuteCommand: true,
  deploymentMaximumPercent: 200,
  deploymentMinimumHealthyPercent: 50,
  deploymentCircuitBreaker: {
    enable: true,
    rollback: true,
  },
  taskDefinitionArgs: {
    container: {
      name: "replication-manager-container",
      image: "rocicorp/zero:canary",
      cpu: 2048,
      memory: 8192,
      essential: true,
      portMappings: [
        {
          name: "change-streamer",
          containerPort: 4849,
          hostPort: 4849,
          appProtocol: "http",
          protocol: "tcp",
        },
      ],
      environment: [
        { name: "AWS_REGION", value: aws.config.region },
        { name: "ZERO_UPSTREAM_DB", value: upstreamDb },
        { name: "ZERO_CVR_DB", value: cvrDb },
        { name: "ZERO_CHANGE_DB", value: changeDb },
        { name: "AWS_ACCESS_KEY_ID", value: awsAccessKeyId },
        { name: "AWS_SECRET_ACCESS_KEY", value: awsSecretAccessKey },
        { name: "ZERO_SCHEMA_JSON", value: schemaJson },
        { name: "ZERO_LOG_FORMAT", value: "json" },
        { name: "ZERO_REPLICA_FILE", value: "sync-replica.db" },
        { name: "ZERO_CHANGE_MAX_CONNS", value: "3" },
        { name: "ZERO_SCHEMA_JSON", value: process.env.ZERO_SCHEMA_JSON },
        { name: "ZERO_NUM_SYNC_WORKERS", value: "0" },
        {
          name: "ZERO_LITESTREAM_BACKUP_URL",
          value: pulumi.interpolate`s3://${backupBucket.id}/backup`,
        },
        { name: "ZERO_AUTH_SECRET", value: jwtSecret },
      ],
      linuxParameters: {
        initProcessEnabled: true,
      },
      stopTimeout: 120,
      logConfiguration: {
        logDriver: "awslogs",
        options: {
          mode: "non-blocking",
          "max-buffer-size": "25m",
          "awslogs-group": replicationManagerLogGroup.name,
          "awslogs-region": aws.config.region,
          "awslogs-stream-prefix": "ecs",
        },
      },
    },
  },
  serviceConnectConfiguration: {
    enabled: true,
    namespace,
    services: [
      {
        portName: "change-streamer",
        clientAlias: [
          {
            port: 4849,
            dnsName: `change-streamer.${namespace}`,
          },
        ],
      },
    ],
  },
});

export const frontendURL = pulumi.interpolate`http://${loadbalancer.loadBalancer.dnsName}`;
export const backupBucketName = backupBucket.id;
