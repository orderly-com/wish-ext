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


class MemberLevel(BaseModel):
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


class LevelLog(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    from_level = models.ForeignKey(MemberLevel, related_name='to_logs', on_delete=models.CASCADE)
    to_level = models.ForeignKey(MemberLevel, related_name='from_logs', on_delete=models.CASCADE)
    clientbase = models.ForeignKey(ClientBase, on_delete=models.CASCADE)
    datetime = models.DateTimeField(null=True)
    from_datetime = models.DateTimeField(null=True)
    to_datetime = models.DateTimeField(null=True)

    source_type = models.CharField(max_length=128)



class Event(BaseModel):
    class Meta:
        indexes = [
            models.Index(fields=['cost_type', ]),
            models.Index(fields=['ticket_type', ]),

            models.Index(fields=['ticket_type', 'cost_type']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    name = models.TextField(blank=False)
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


class EventLog(BaseModel):

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    event = models.ForeignKey(Event, related_name='logs', on_delete=models.CASCADE)
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



@client_info_model
class WishInfo(BaseModel):

    clientbase = models.OneToOneField(ClientBase, related_name='wish_info', blank=False, on_delete=models.CASCADE)
    level = models.ForeignKey(MemberLevel, related_name='clientbases', null=True, blank=True)

    def __getattr__(self, attr):
        if attr == 'readbase_set':
            return TeamMongoDB(self.clientbase.team).readbases.filter(clientbase_id=self.clientbase_id)

    def get_sum_of_total_read(self):
        qs = self.clientbase.wish_info.readbase_set

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)
        if total_progress is None:
            total_progress = 0

        return total_progress

    def get_count_of_total_article(self):
        return self.clientbase.wish_info.readbase_set.filter(F('articlebase_id') != None).values('path').count()

    def get_avg_of_each_read(self):
        qs = self.clientbase.wish_info.readbase_set.filter(F('articlebase_id') != None)
        if not qs.count():
            return 0

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)

        return total_progress / qs.count()

    def get_times_of_read(self):
        return self.clientbase.wish_info.readbase_set.filter(F('articlebase_id') != None).count()

    def first_read(self):
        first_readbase = self.clientbase.wish_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').first()
        if first_readbase:
            return first_readbase['datetime']
        else:
            return None

    def last_read(self):
        last_readbase = self.clientbase.wish_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').last()
        if last_readbase:
            return last_readbase['datetime']
        else:
            return None
