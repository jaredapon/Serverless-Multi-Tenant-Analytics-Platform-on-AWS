"""
Teleo Tribe – handler.py

This Lambda function extracts daily analytics data from Teleo's
tenant database, converts it into a flat CSV model, and uploads
the file to the Teleo tribe's S3 bucket for visualization in Power BI.
"""

import os
import sys
from pathlib import Path

# Add the template directory to the Python path
template_dir = Path(__file__).parent.parent.parent / 'integreat_analytics' / 'template'
sys.path.append(str(template_dir))

from tenant_handler_template import export_and_upload, lambda_handler

# Teleo-specific configuration
TENANT = 'teleo'

def handler(event, context):
    """
    AWS Lambda handler for Teleo analytics export.
    
    Args:
        event: AWS Lambda event
        context: AWS Lambda context
        
    The event can contain:
    - date_str: Optional date string in YYYY-MM-DD format
    """
    # Add tenant to event
    event['tenant'] = TENANT
    return lambda_handler(event, context)
