#!/usr/bin/env python
#**    This line is 79 characters long.  The 80th character should wrap.   ***\

#imports:
import json
import pytz
from datetime import datetime
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc

#define:
def initialTransform(request):
    """Entry point for HTTP Cloud Function

    Instantiates a workflow template for flexibility, so that instantiation can
        be triggered by schedule or manually.

    Args:
        request (flask.Request): The request object

    Returns:
        'Success'

    """

    
    return 'Success'

#run:
if __name__ == '__main__':
    pass
