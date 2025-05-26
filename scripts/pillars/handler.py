"""
Pillars Tribe – handler.py

Pillars' analytics Lambda connects to its NeonDB database,
generates a flat CSV file from recent API activity, and
uploads the report to Pillars' assigned S3 bucket.
"""

from integreat_analytics.template.tenant_handler_template import export_and_upload, lambda_handler

TENANT = 'pillars'

def handler(event, context):
    """
    AWS Lambda handler for Pillars analytics export.
    The event can contain:
    - date_str: Optional date string in YYYY-MM-DD format
    - last_export_time: Optional last export time in YYYY-MM-DDTHH:MM:SS format
    """
    event['tenant'] = TENANT
    return lambda_handler(event, context)
