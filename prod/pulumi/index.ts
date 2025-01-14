import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";

const cluster = new aws.ecs.Cluster("zero-cache-cluster", {
  settings: [
    {
      name: "containerInsights",
      value: "enhanced",
    },
  ],
  serviceConnectDefaults: {
    namespace: "zero-cache-prod",
  },

  configuration: {
    executeCommandConfiguration: {
      logging: "OVERRIDE",
      logConfiguration: {
        cloudWatchLogGroupName: `/ecs/${name}`,
        cloudWatchEncryptionEnabled: true,
        s3BucketName: "zero-cache-logs",
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

new awsx.ecs.FargateService(`view-syncer`, {
  cluster: cluster.arn,
  desiredCount: 1,
  assignPublicIp: true,
  taskDefinitionArgs: {
    container: {
      name: "awsx-ecs",
      image: "your-ecr-repo:latest",
      cpu: 8192,
      memory: 16384,
      portMappings: [
        {
          containerPort: 4848,
          targetGroup: loadbalancer.defaultTargetGroup,
        },
      ],
      healthCheck: {
        command: ["CMD-SHELL", "curl -f http://localhost:4848/ || exit 1"],
        interval: 5,
        retries: 3,
        startPeriod: 300,
      },
    },
  },
  serviceConnectConfiguration: {
    enabled: true,
    namespace: "zero-cache-prod",
    services: [
      {
        portName: "view-syncer",
        clientAlias: [
          {
            port: 4848,
            dnsName: "view-syncer.zero-cache-prod",
          },
        ],
      },
    ],
  },
});

new awsx.ecs.FargateService(`$replication-manager`, {
  desiredCount: 1,
  assignPublicIp: true,
  taskDefinitionArgs: {
    container: {
      name: "awsx-ecs",
      image: "your-ecr-repo:latest",
      cpu: 2048,
      memory: 8192,
      portMappings: [
        {
          containerPort: 4849,
          targetGroup: loadbalancer.defaultTargetGroup,
        },
      ],
    },
  },
  serviceConnectConfiguration: {
    enabled: true,
    namespace: "zero-cache-prod",
    services: [
      {
        portName: "change-streamer",
        clientAlias: [
          {
            port: 4849,
            dnsName: "change-streamer.zero-cache-prod",
          },
        ],
      },
    ],
  },
});

export const frontendURL = pulumi.interpolate`http://${loadbalancer.loadBalancer.dnsName}`;
