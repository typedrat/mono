import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";

const config = new pulumi.Config("zero-cache");
const namespace = config.require("namespace");

const cluster = new aws.ecs.Cluster(`${namespace}-cluster`, {
  settings: [
    {
      name: "containerInsights",
      value: "enhanced",
    },
  ],
  serviceConnectDefaults: {
    namespace
  },

  configuration: {
    executeCommandConfiguration: {
      logging: "OVERRIDE",
      logConfiguration: {
        cloudWatchLogGroupName: `/ecs/${namespace}-cluster`,
        cloudWatchEncryptionEnabled: true,
        s3BucketName: `${namespace}-logs`,
      },
    },
  },
});

const loadbalancer = new awsx.lb.ApplicationLoadBalancer("loadbalancer", {
  defaultTargetGroup: {
    port: 4848,
    protocol: "HTTP",
    healthCheck: {
      enabled: true,
      path: "/",
      protocol: "HTTP",
      port: "4850",
      healthyThreshold: 3,
      unhealthyThreshold: 3,
      timeout: 60,
      interval: 300,
      matcher: "200",
    },
    targetType: "ip",
    deregistrationDelay: 300,
    stickiness: {
      enabled: false,
      type: "lb_cookie",
    },
  },
});


// View Syncer Service
new awsx.ecs.FargateService("view-syncer", {
    cluster: cluster.arn,
    desiredCount: 10,
    assignPublicIp: true,
    enableExecuteCommand: true,
    healthCheckGracePeriodSeconds: 300,
    deploymentMaximumPercent: 120,
    deploymentMinimumHealthyPercent: 50,
    deploymentCircuitBreaker: {
        enable: true,
        rollback: true
    },
    taskDefinitionArgs: {
        container: {
            name: "view-syncer-container",
            image: "rocicorp/zero:canary",
            cpu: 8192,
            memory: 16384,
            essential: true,
            portMappings: [{
                containerPort: 4848,
                targetGroup: loadbalancer.defaultTargetGroup,
            }],
            healthCheck: {
                command: ["CMD-SHELL", "curl -f http://localhost:4848/ || exit 1"],
                interval: 5,
                retries: 3,
                startPeriod: 300,
            },
            environment: [
                { name: "AWS_REGION", value: aws.config.region },
                { name: "ZERO_UPSTREAM_DB", value: process.env.ZERO_UPSTREAM_DB },
                { name: "ZERO_CVR_DB", value: process.env.ZERO_CVR_DB },
                { name: "ZERO_CHANGE_DB", value: process.env.ZERO_CHANGE_DB },
                { name: "ZERO_JWT_SECRET", value: process.env.ZERO_JWT_SECRET },
                { name: "AWS_ACCESS_KEY_ID", value: process.env.AWS_ACCESS_KEY_ID },
                { name: "AWS_SECRET_ACCESS_KEY", value: process.env.AWS_SECRET_ACCESS_KEY },
                { name: "ZERO_CHANGE_STREAMER_URI", value: `ws://change-streamer.${namespace}:4849` },
                { name: "ZERO_LOG_FORMAT", value: "json" },
                { name: "ZERO_REPLICA_FILE", value: "/data/db/sync-replica.db" },
            ],
            linuxParameters: {
                initProcessEnabled: true,
            },
            stopTimeout: 120,
            logConfiguration: {
                logDriver: "awslogs",
                options: {
                    "mode": "non-blocking",
                    "max-buffer-size": "25m",
                    "awslogs-group": `/ecs/view-syncer`,
                    "awslogs-region": aws.config.region,
                    "awslogs-stream-prefix": "ecs"
                }
            },
        },
    },
    serviceConnectConfiguration: {
        enabled: true,
        namespace,
        services: [{
            portName: "view-syncer",
            clientAlias: [{
                port: 4848,
                dnsName: `view-syncer.${namespace}`
            }]
        }]
    }
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
        rollback: true
    },
    taskDefinitionArgs: {
        container: {
            name: "replication-manager-container",
            image: "rocicorp/zero:canary",
            cpu: 2048,
            memory: 8192,
            essential: true,
            portMappings: [{
                containerPort: 4849,
                targetGroup: loadbalancer.defaultTargetGroup,
            }],
            environment: [
                { name: "AWS_REGION", value: aws.config.region },
                { name: "ZERO_UPSTREAM_DB", value: process.env.ZERO_UPSTREAM_DB },
                { name: "ZERO_CVR_DB", value: process.env.ZERO_CVR_DB },
                { name: "ZERO_CHANGE_DB", value: process.env.ZERO_CHANGE_DB },
                { name: "ZERO_JWT_SECRET", value: process.env.ZERO_JWT_SECRET },
                { name: "AWS_ACCESS_KEY_ID", value: process.env.AWS_ACCESS_KEY_ID },
                { name: "AWS_SECRET_ACCESS_KEY", value: process.env.AWS_SECRET_ACCESS_KEY },
                { name: "ZERO_LOG_FORMAT", value: "json" },
                { name: "ZERO_REPLICA_FILE", value: "/data/db/sync-replica.db" },
            ],
            linuxParameters: {
                initProcessEnabled: true,
            },
            stopTimeout: 120,
            logConfiguration: {
                logDriver: "awslogs",
                options: {
                    "mode": "non-blocking",
                    "max-buffer-size": "25m",
                    "awslogs-group": `/ecs/replication-manager`,
                    "awslogs-region": aws.config.region,
                    "awslogs-stream-prefix": "ecs"
                }
            },
        },
    },
    serviceConnectConfiguration: {
        enabled: true,
        namespace,
        services: [{
            portName: "change-streamer",
            clientAlias: [{
                port: 4849,
                dnsName: `change-streamer.${namespace}`
            }]
        }]
    }
});

export const frontendURL = pulumi.interpolate`http://${loadbalancer.loadBalancer.dnsName}`;
