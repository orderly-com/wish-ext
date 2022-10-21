from django.urls import reverse
import html2text
from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource

from core.models import BaseModel, ValueTaggable
from team.models import Team, OrderBase, ProductBase, ClientBase
from team.registries import client_info_model
from tag_assigner.registries import taggable

from cerem.utils import TeamMongoDB, F, Sum

from ..extension import wish_ext


class MemberLevelBase(BaseModel):
    class Meta:

        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['name', ]),

            models.Index(fields=['team', 'name']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)

    external_id = models.TextField(blank=False) # 對應代碼

    rank = models.IntegerField(default=0) # 1
    uuid = models.UUIDField(default=uuid4, unique=True)

    name = models.TextField(blank=False) # 等級名稱
    removed = models.BooleanField(default=False)


class LevelLogBase(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    from_level = models.ForeignKey(MemberLevelBase, related_name='to_logs', on_delete=models.CASCADE)
    to_level = models.ForeignKey(MemberLevelBase, related_name='from_logs', on_delete=models.CASCADE)
    clientbase = models.ForeignKey(ClientBase, on_delete=models.CASCADE)
    datetime = models.DateTimeField(null=True)
    from_datetime = models.DateTimeField(null=True)
    to_datetime = models.DateTimeField(null=True)

    source_type = models.CharField(max_length=128)
    removed = models.BooleanField(default=False)


class EventBase(BaseModel):
    class Meta:
        indexes = [
            models.Index(fields=['cost_type', ]),
            models.Index(fields=['ticket_type', ]),

            models.Index(fields=['ticket_type', 'cost_type']),
        ]

    external_id = models.TextField(blank=False) # 對應代碼
    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    name = models.TextField(blank=False)
    ticket_name = models.TextField(blank=False)
    ticket_type = models.TextField(blank=False)

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
    removed = models.BooleanField(default=False)

    attributions = JSONField(blank=True, null=True)


class EventLogBase(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    event = models.ForeignKey(EventBase, related_name='logs', on_delete=models.CASCADE)
    clientbase = models.ForeignKey(ClientBase, on_delete=models.CASCADE)
    datetime = models.DateTimeField(null=True)

    ACTION_CLAIM = 'claim'
    ACTION_USE = 'use'

    ACTION_CHOICES = (
        (ACTION_CLAIM, '領取'),
        (ACTION_USE, '使用'),
    )

    cost_type = models.CharField(
        choices=ACTION_CHOICES,
        default=ACTION_CLAIM,
        max_length=64,
        blank=False,
        null=False,
    )
    removed = models.BooleanField(default=False)

    attributions = JSONField(blank=True, null=True)


class PointLogBase(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    point_name = models.TextField(blank=False)
    clientbase = models.ForeignKey(ClientBase, on_delete=models.CASCADE)
    datetime = models.DateTimeField(null=True)
    amount = models.IntegerField(default=0) # 1

    is_transaction = models.BooleanField(default=False)
    removed = models.BooleanField(default=False)

    attributions = JSONField(blank=True, null=True)


@client_info_model
class WishInfo(BaseModel):

    LEVEL_UP = 'up'
    LEVEL_STAY = 'stay'
    LEVEL_DOWN = 'down'

    LEVEL_DIRECTION_CHOICES = (
        (LEVEL_UP, '升階'),
        (LEVEL_STAY, '續等'),
        (LEVEL_DOWN, '降等'),
    )

    last_level_direction = models.CharField(
        choices=LEVEL_DIRECTION_CHOICES,
        max_length=64,
        blank=True,
        null=True,
    )
    clientbase = models.OneToOneField(ClientBase, related_name='wish_info', blank=False, on_delete=models.CASCADE)
    level = models.ForeignKey(MemberLevelBase, related_name='clientbases', null=True, blank=True, on_delete=models.CASCADE)

    def __getattr__(self, attr):
        if attr == 'readbase_set':
            return TeamMongoDB(self.clientbase.team).readbases.filter(clientbase_id=self.clientbase_id)

    attributions = JSONField(blank=True, null=True)
