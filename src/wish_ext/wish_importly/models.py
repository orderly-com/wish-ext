from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource
from team.models import Team

from core.models import BaseModel, ValueTaggable
from importly.models import RawModel


class Event(BaseModel):
    class Meta:
        indexes = [
            models.Index(fields=['cost_type', ]),
            models.Index(fields=['ticket_type', ]),

            models.Index(fields=['ticket_type', 'cost_type']),
        ]
    external_id = models.TextField(blank=False)

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    name = models.TextField(blank=False)
    ticket_type = models.TextField(blank=False)
    ticket_name = models.TextField(blank=False)

    COST_TYPE_FREE = 'free'
    COST_TYPE_CODE = 'code'
    COST_TYPE_CREDIT = 'credit'

    COST_TYPE_CHOICES = (
        (COST_TYPE_FREE, '免費'),
        (COST_TYPE_CODE, '兌換碼'),
        (COST_TYPE_CREDIT, '點數'),
    )

    cost_type = models.CharField(
        choices=COST_TYPE_CHOICES,
        default=COST_TYPE_FREE,
        max_length=64,
        blank=False,
        null=False,
    )

    attributions = JSONField(blank=True, null=True)


class EventLog(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    event_external_id = models.TextField(blank=False)
    clientbase_external_id = models.TextField(blank=False)
    datetime = models.DateTimeField(null=True)

    ACTION_CLAIM = 'claim'
    ACTION_USE = 'use'

    ACTION_CHOICES = (
        (ACTION_CLAIM, '領取'),
        (ACTION_USE, '使用'),
    )

    action = models.CharField(
        choices=ACTION_CHOICES,
        default=ACTION_CLAIM,
        max_length=64,
        blank=False,
        null=False,
    )

    attributions = JSONField(blank=True, null=True)

class Level(BaseModel):
    class Meta:

        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['name', ]),

            models.Index(fields=['team', 'name']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)

    external_id = models.TextField(blank=False) # 對應代碼

    rank = models.IntegerField(default=0) # 1

    name = models.TextField(blank=False) # 等級名稱
    attributions = JSONField(blank=True, null=True)


class LevelLog(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    from_level_id = models.TextField(blank=False)
    to_level_id = models.TextField(blank=False)
    clientbase_external_id = models.TextField(blank=False)
    datetime = models.DateTimeField(null=True)
    from_datetime = models.DateTimeField(null=True)
    to_datetime = models.DateTimeField(null=True)

    source_type = models.CharField(max_length=128)
    attributions = JSONField(blank=True, null=True)

