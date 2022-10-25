from django.urls import reverse
import html2text
from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource

from core.models import BaseModel, ValueTaggable
from team.models import Team, OrderBase, AbstractProduct, ClientBase
from team.registries import client_info_model
from tag_assigner.registries import taggable

from cerem.utils import TeamMongoDB, F, Sum

from ..extension import retail_ext
from ..wish.models import Brand


class ProductCategory(BaseModel):
    class Meta:

        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['name', ]),

            models.Index(fields=['team', 'name']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)

    external_id = models.TextField(blank=False)
    uuid = models.UUIDField(default=uuid4, unique=True)

    name = models.TextField(blank=False)
    removed = models.BooleanField(default=False)


@retail_ext.ProductModel
@taggable('product')
class ProductBase(AbstractProduct):
    class Meta:
        indexes = [
            models.Index(fields=['datasource', ]),

            models.Index(fields=['team', 'datasource']),
        ]

    price = models.FloatField(default=0)

    datasource = models.ForeignKey(DataSource, blank=False, default=1, on_delete=models.CASCADE)

    attributions = JSONField(default=dict)
    categories = models.ManyToManyField(ProductCategory, blank=True)

    def get_detail_url(self):
        return reverse('media:article-detail', kwargs={'uuid': self.uuid})

    def get_records_url(self):
        return reverse('media:article-records', kwargs={'uuid': self.uuid})


@retail_ext.OrderModel
@taggable('order')
class PurchaseBase(OrderBase):
    class Meta:
        indexes = [
            models.Index(fields=['datasource', ]),

            models.Index(fields=['team', 'datasource']),
        ]

    STATUS_CONFIRMED = 'CONFIRMED'
    STATUS_ABANDONED = 'ABANDONED'
    STATUS_KEEP = 'KEEP'

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    brand = models.ForeignKey(Brand, blank=False, null=True, on_delete=models.CASCADE)
    total_price = models.FloatField(default=0)

    datasource = models.ForeignKey(DataSource, blank=False, default=1, on_delete=models.CASCADE)

    attributions = JSONField(default=dict)
    categories = models.ManyToManyField(ProductCategory, blank=True)
    status = models.CharField(max_length=64, blank=False, null=False, default=STATUS_CONFIRMED)

    @classmethod
    def confirmed_objects(cls):
        return cls.objects.filter(status=cls.STATUS_CONFIRMED)

    def get_detail_url(self):
        return reverse('media:article-detail', kwargs={'uuid': self.uuid})

    def get_records_url(self):
        return reverse('media:article-records', kwargs={'uuid': self.uuid})


class OrderProduct(BaseModel):

    class Meta:
        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['productbase', ]),
            models.Index(fields=['purchasebase', ]),
            models.Index(fields=['clientbase', ]),

            models.Index(fields=['team', 'productbase']),
            models.Index(fields=['team', 'purchasebase']),
            models.Index(fields=['team', 'clientbase']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    datalist_id = models.IntegerField(blank=True, null=True)  # so we can trace back to it's original orderlist

    productbase = models.ForeignKey(ProductBase, blank=False, null=False, on_delete=models.CASCADE)
    purchasebase = models.ForeignKey(PurchaseBase, blank=False, null=False, on_delete=models.CASCADE)
    clientbase = models.ForeignKey(ClientBase, blank=False, null=True, default=None, on_delete=models.CASCADE)

    refound = models.BooleanField(default=False)

    sale_price = models.FloatField(default=0.0)
    quantity = models.IntegerField(default=0)
    total_price = models.FloatField(default=0.0)


@client_info_model
class RetailInfo(BaseModel):

    clientbase = models.OneToOneField(ClientBase, related_name='media_info', blank=False, on_delete=models.CASCADE)

    # aggregated data, for performance purposes
    reading_rank = models.IntegerField(default=0)
    article_count = models.IntegerField(default=0)
    last_read_datetime = models.DateTimeField(null=True, blank=True)
    avg_reading_progress = models.FloatField(default=0)

    def __getattr__(self, attr):
        if attr == 'readbase_set':
            return TeamMongoDB(self.clientbase.team).readbases.filter(clientbase_id=self.clientbase_id)

    def get_sum_of_total_read(self):
        qs = self.clientbase.media_info.readbase_set

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)
        if total_progress is None:
            total_progress = 0

        return total_progress

    def get_count_of_total_article(self):
        return self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).values('path').count()

    def get_avg_of_each_read(self):
        qs = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None)
        if not qs.count():
            return 0

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)

        return total_progress / qs.count()

    def get_times_of_read(self):
        return self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).count()

    def first_read(self):
        first_readbase = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').first()
        if first_readbase:
            return first_readbase['datetime']
        else:
            return None

    def last_read(self):
        last_readbase = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').last()
        if last_readbase:
            return last_readbase['datetime']
        else:
            return None
