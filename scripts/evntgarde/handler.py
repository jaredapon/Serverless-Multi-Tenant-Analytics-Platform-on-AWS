"""
EventGarde Tribe – handler.py

This Lambda queries EventGarde's NeonDB for current analytics,
exports a flat snapshot to CSV, and uploads it to their S3 bucket
for consumption by Power BI dashboards.
"""

from integreat_analytics.template.tenant_handler_template import export_and_upload, lambda_handler

TENANT = 'evntgarde'

def handler(event, context):
    """
    AWS Lambda handler for Evntgarde analytics export.
    The event can contain:
    - date_str: Optional date string in YYYY-MM-DD format
    - last_export_time: Optional last export time in YYYY-MM-DDTHH:MM:SS format
    """
    event['tenant'] = TENANT
    return lambda_handler(event, context)