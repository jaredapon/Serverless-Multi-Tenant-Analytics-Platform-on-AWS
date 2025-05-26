"""
tenant_lambda_stack.py

This CDK stack creates Lambda functions for each tenant to handle their analytics exports.

Responsibilities:
- Defines a PythonFunction Lambda with its code under scripts/<tenant>/handler.py
- Configures the Lambda with:
    • 2048 MB of memory (2GB)
    • 15-minute timeout (maximum allowed for AWS Lambda)
    • This also scales the vCPU to approximately 1 vCPU for the duration
- Passes environment/context variables to the Lambda for runtime config

Each tenant gets its own Lambda to run analytics exports independently.
This keeps data pipelines isolated and allows for custom logic per tenant.

Note:
- AWS Lambda does not automatically scale CPU or RAM per invocation
- CPU is scaled proportionally to memory (2GB ≈ 1 vCPU)
- To improve performance, memory must be manually increased
- S3 access is assumed to be provisioned and granted in a separate stack
"""

import os
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_iam as iam,
    Duration,
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct

class TenantLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define tenants and their configurations
        tenants = {
            'teleo': {
                'handler': 'scripts/teleo/handler.py',
                'description': 'Teleo analytics export Lambda'
            },
            'pillars': {
                'handler': 'scripts/pillars/handler.py',
                'description': 'Pillars analytics export Lambda'
            },
            'campus': {
                'handler': 'scripts/campus/handler.py',
                'description': 'Campus analytics export Lambda'
            },
            'evntgarde': {
                'handler': 'scripts/evntgarde/handler.py',
                'description': 'EventGarde analytics export Lambda'
            }
        }

        # Create Lambda functions for each tenant
        for tenant, config in tenants.items():
            # Create Lambda function
            fn = PythonFunction(
                self, f"{tenant.title()}AnalyticsLambda",
                entry=".",  # Root directory containing both scripts/ and integreat_analytics/
                index=config['handler'],
                runtime=lambda_.Runtime.PYTHON_3_9,
                memory_size=2048,  # 2GB
                timeout=Duration.minutes(15),  # 15 minutes
                description=config['description'],
                function_name=f"{tenant}-analytics-handler",  # Explicit function name
                environment={
                    'DATABASE_URL': os.getenv('DATABASE_URL', ''),
                },
                bundling={
                    'command': [
                        'bash', '-c',
                        'pip install -r requirements-lambda.txt -t /asset-output && cp -au /asset-input/scripts /asset-input/integreat_analytics /asset-input/requirements-lambda.txt /asset-output/ && cp /asset-input/.env /asset-output/'
                    ]
                }
            )

            # Grant S3 access to the Lambda
            bucket_names = {
                "campus": "campus-student-lifecycle-tenant-bucket",
                "evntgarde": "evntgarde-event-management-tenant-bucket",
                "pillars": "pillars-edu-quality-assessor-tenant-bucket",
                "teleo": "teleo-church-application-tenant-bucket",
            }
            bucket_name = bucket_names[tenant]
            fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        's3:PutObject',
                        's3:GetObject',
                        's3:ListBucket'
                    ],
                    resources=[
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                )
            )
