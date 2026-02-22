import os
DQ = chr(34)
DQ3 = 3 * DQ
content = '''
{DQ3} 
Unit tests for app/core/export_service.py 
 
All tests are fully offline (no DB, no network). 
{DQ3} 
import os 
import json 
import pytest 
from datetime import datetime, timedelta 
from unittest.mock import MagicMock, patch 
 
from app.core.export_service import ExportService 
from app.core.models import ExportJob, ExportFormat, ExportStatus
