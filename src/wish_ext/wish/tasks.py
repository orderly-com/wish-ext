import re
import gc
import datetime

from django.conf import settings
from django.utils import timezone
from config.celery import app

from cerem.tasks import fetch_site_tracking_data
from cerem.utils import kafka_headers

from datahub.models import DataSync
from team.models import Team
from tag_assigner.models import TagAssigner, ValueTag

from core.utils import run

from cerem.tasks import insert_to_cerem, aggregate_from_cerem

from ..extension import wish_ext

@wish_ext.periodic_task()
def sync_clientbase_level(**kwargs):
    pass
