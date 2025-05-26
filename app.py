#!/usr/bin/env python3

import os
import aws_cdk as cdk
from integreat_analytics.tenant_lambda_stack import TenantLambdaStack
from integreat_analytics.eventbridge_stack import EventBridgeStack

#from integreat_analytics.integreat_analytics_stack import InteGreatAnalyticsStack


app = cdk.App()

# Deploy tenant Lambda functions
lambda_stack = TenantLambdaStack(app, "TenantLambdaStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

# Deploy EventBridge rules to schedule the Lambdas
EventBridgeStack(app, "EventBridgeStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

app.synth()

"""
app.py

Main CDK entrypoint for deploying Integreat's analytics infrastructure.

Instructions for developers:
- Add or remove tenants in the TENANTS dictionary as needed.
  • Each tenant must specify their S3 bucket name and the path to their Python handler.
- Each tenant will be deployed with:
  • A Python Lambda function for exporting analytics to their S3 bucket
- All tenant Lambdas will be scheduled by a single EventBridge rule that triggers daily at 12MN (GMT+8).
- Integreat's own Lambda (in scripts/integreat) runs the centralized DW ETL and uploads CSVs to each tenant bucket.
- No secrets manager is used; configuration is passed via CDK context or environment variables.
- S3 permissions must be managed in the separate Node.js infrastructure stack.
"""