"""
Campus Tribe – handler.py

This Lambda function connects to the Campus NeonDB instance,
queries the latest analytics data, exports it to CSV, and uploads
it to the Campus tribe's dedicated S3 bucket for Power BI reporting.
"""

from integreat_analytics.template.tenant_handler_template import export_and_upload, lambda_handler

TENANT = 'campus'

def handler(event, context):
    """
    AWS Lambda handler for Campus analytics export.
    The event can contain:
    - date_str: Optional date string in YYYY-MM-DD format
    - last_export_time: Optional last export time in YYYY-MM-DDTHH:MM:SS format
    """
    event['tenant'] = TENANT
    return lambda_handler(event, context)