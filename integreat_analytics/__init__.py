"""
integreat_analytics

This package defines the CDK infrastructure for the Integreat analytics system.

Responsibilities:
- Provision tenant-specific analytics Lambdas (per handler.py)
- Schedule those Lambdas using EventBridge (runs nightly at 12MN)
- Pass runtime configuration via CDK context or environment variables
- Does not rely on AWS Secrets Manager (secure values are handled outside)

Stacks included:
- tenant_lambda_stack.py: Deploys per-tenant analytics Lambda
- eventbridge_stack.py: Schedules the Lambda with a cron rule
"""
