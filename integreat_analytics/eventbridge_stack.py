"""
eventbridge_stack.py

This CDK stack creates an EventBridge rule (cron scheduler) that triggers
a target Lambda function at a defined interval (e.g., daily at 12 midnight).

Responsibilities:
- Defines a rule using EventBridge Schedule (cron)
- Binds the rule to the specified tenant Lambda

This scheduler is used to automate analytics extraction and upload for each tenant.
The trigger time is set to match the operational timezone (e.g., GMT+8 for 12MN).
"""

from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct
from aws_cdk import Duration
import os

class EventBridgeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        tenants = ['campus', 'evntgarde', 'pillars', 'teleo']

        # Create a single rule for all analytics exports
        analytics_rule = events.Rule(
            self, "AnalyticsDailyRule",
            schedule=events.Schedule.cron(
                minute='0',
                hour='16',  # 16 UTC = 00:00 UTC+8
                day='*',
                month='*',
                year='*'
            ),
            description="Daily trigger for all analytics exports"
        )

        # Add each tenant Lambda as a target
        for tenant in tenants:
            fn = lambda_.Function.from_function_name(
                self, f"{tenant.title()}AnalyticsLambda",
                function_name=f"{tenant}-analytics-handler"
            )
            analytics_rule.add_target(targets.LambdaFunction(fn))

        # Optionally, add the Integreat ETL Lambda as a target too
        etl_lambda = PythonFunction(
            self, "IntegreatETLLambda",
            entry=".",  # Root directory containing scripts/
            index="scripts/integreat/integreat_pipeline.py",
            handler="ETL_Handler",  # This is the function you defined
            runtime=lambda_.Runtime.PYTHON_3_9,
            memory_size=2048,
            timeout=Duration.minutes(15),
            description="Integreat ETL pipeline Lambda",
            function_name="integreat-pipeline-handler",
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
        analytics_rule.add_target(targets.LambdaFunction(etl_lambda))