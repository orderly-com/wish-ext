from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource
from team.models import Team

from core.models import BaseModel, ValueTaggable
from importly.models import RawModel
