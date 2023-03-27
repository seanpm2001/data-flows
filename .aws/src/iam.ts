// Module to abstract IAM elements needed for Prefect v2
import { DataAwsCallerIdentity } from '@cdktf/provider-aws/lib/data-aws-caller-identity';
import {
  DataAwsIamPolicyDocument,
  DataAwsIamPolicyDocumentStatement
} from '@cdktf/provider-aws/lib/data-aws-iam-policy-document';
import { DataAwsSecretsmanagerSecret } from '@cdktf/provider-aws/lib/data-aws-secretsmanager-secret';
import { Construct } from 'constructs';
import { DataAwsRegion } from '@cdktf/provider-aws/lib/data-aws-region';
import { S3Bucket } from '@cdktf/provider-aws/lib/s3-bucket';
import { IamRole } from '@cdktf/provider-aws/lib/iam-role';

// Custom construct to create IAM roles needed for a Prefect v2 Agent
export class AgentIamPolicies extends Construct {
  public readonly agentExecutionPolicyStatements: DataAwsIamPolicyDocumentStatement[];
  public readonly agentTaskPolicyStatements: DataAwsIamPolicyDocumentStatement[];
  private readonly ecsAppPrefix: string;
  private readonly caller: DataAwsCallerIdentity;
  private readonly region: DataAwsRegion;
  constructor(
    scope: Construct,
    name: string,
    dockerSecret: DataAwsSecretsmanagerSecret,
    prefectV2Secret: DataAwsSecretsmanagerSecret,
    ecsAppPrefix: string,
    caller: DataAwsCallerIdentity,
    region: DataAwsRegion
  ) {
    super(scope, name);
    this.caller = caller;
    this.region = region;
    this.ecsAppPrefix = ecsAppPrefix;
    // create an inline policy doc for the execution role that can be combined with AWS managed policy
    this.agentExecutionPolicyStatements = [
      {
        actions: [
          'kms:Decrypt',
          'secretsmanager:GetSecretValue',
          'ssm:GetParameters'
        ],
        effect: 'Allow',
        resources: [dockerSecret.arn, prefectV2Secret.arn]
      }
    ];

    // build the policy document for the Task role using Policy statement functions
    this.agentTaskPolicyStatements = [
      this.getAgentTaskAllAccess(),
      this.getAgentTaskEcsAccess(),
      this.getAgentTaskIamAccess()
    ];
  }
  // build policy statment for resources "*"
  private getAgentTaskAllAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: [
        'ecs:RegisterTaskDefinition',
        'ecs:ListTaskDefinitions',
        'ecs:DescribeTaskDefinition',
        'ecs:DeregisterTaskDefinition'
      ],
      effect: 'Allow',
      resources: ['*']
    };
  }
  // build policy statment for ECS task actions
  private getAgentTaskEcsAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['ecs:StopTask', 'ecs:RunTask'],
      effect: 'Allow',
      resources: ['*'],
      condition: [
        {
          test: 'ArnLike',
          values: [
            `arn:aws:ecs:${this.region.name}:${this.caller.accountId}:cluster/${this.ecsAppPrefix}*`
          ],
          variable: 'ecs:cluster'
        }
      ]
    };
  }
  // build policy statement that allows agent to pass the Prefect IAM roles
  private getAgentTaskIamAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['iam:PassRole'],
      effect: 'Allow',
      resources: [
        `arn:aws:iam::${this.caller.accountId}:role/${this.ecsAppPrefix}*`
      ]
    };
  }
}

export class DataFlowsIamRoles extends Construct {
  private readonly fileSystem: S3Bucket;
  private readonly caller: DataAwsCallerIdentity;
  private readonly region: DataAwsRegion;
  constructor(
    scope: Construct,
    name: string,
    fileSystem: S3Bucket,
    caller: DataAwsCallerIdentity,
    region: DataAwsRegion,
    environment: string,
    deploymentType: string
  ) {
    super(scope, name);
    this.caller = caller;
    this.region = region;
    this.fileSystem = fileSystem;
    // create an inline policy doc for the execution role that can be combined with AWS managed policy
    const flowExecutionPolicyStatements = [
      {
        actions: [
          'kms:Decrypt',
          'secretsmanager:GetSecretValue',
          'ssm:GetParameters'
        ],
        effect: 'Allow',
        resources: [
          `arn:aws:secretsmanager:${this.region.name}:${this.caller.accountId}:secret:dpt/${environment}/data_flows_prefect_*`
        ]
      }
    ];

    // build the policy document for the Task role using Policy statement functions
    const flowTaskPolicyStatements = [
      this.getFlowS3BucketAccess(),
      this.getFlowS3ObjectAccess()
    ];

    this.createFlowIamRole(
      `data-flows-prefect-${deploymentType}-exec-role`,
      flowExecutionPolicyStatements
    );
    this.createFlowIamRole(
      `data-flows-prefect-${deploymentType}-task-role`,
      flowTaskPolicyStatements
    );
  }
  // build policy statment for S3 bucket access
  private getFlowS3BucketAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:ListBucket'],
      effect: 'Allow',
      resources: [this.fileSystem.arn]
    };
  }
  // build policy statment for S3 object access
  private getFlowS3ObjectAccess(): DataAwsIamPolicyDocumentStatement {
    return {
      actions: ['s3:*Object'],
      effect: 'Allow',
      resources: [`${this.fileSystem.arn}/data/*`]
    };
  }
  // build policy statment for S3 object access
  private getFlowAssumeRoleAccess(role_name: string): DataAwsIamPolicyDocument {
    return new DataAwsIamPolicyDocument(this, `${role_name}TrustPolicy`, {
      version: '2012-10-17',
      statement: [
        {
          effect: 'Allow',
          actions: ['sts:AssumeRole'],
          principals: [
            {
              identifiers: ['ecs-tasks.amazonaws.com'],
              type: 'Service'
            }
          ]
        }
      ]
    });
  }
  private getFlowRolePolicy(
    role_name: string,
    policyStatements: DataAwsIamPolicyDocumentStatement[]
  ): DataAwsIamPolicyDocument {
    return new DataAwsIamPolicyDocument(this, `${role_name}AccessPolicy`, {
      version: '2012-10-17',
      statement: policyStatements
    });
  }

  private createFlowIamRole(
    name: string,
    policy: DataAwsIamPolicyDocumentStatement[]
  ): IamRole {
    const inline_policy = this.getFlowRolePolicy(name, policy);
    return new IamRole(this, name, {
      name: name,
      assumeRolePolicy: this.getFlowAssumeRoleAccess(name).json,
      inlinePolicy: [
        {
          name: `${name}-access-policy`,
          policy: inline_policy.json
        }
      ]
    });
  }
}