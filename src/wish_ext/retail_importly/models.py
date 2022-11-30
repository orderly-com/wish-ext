from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource
from team.models import Team

from core.models import BaseModel, ValueTaggable
from importly.models import RawModel

from ..retail.models import RetailProduct, PurchaseBase


class Product(RawModel):

    class Meta:
        indexes = [
            models.Index(fields=['team', ]),
        ]

    external_id = models.TextField(blank=False)
    name = models.CharField(max_length=64, default=str)

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    uuid = models.UUIDField(default=uuid4, unique=True)


    price = models.FloatField(default=0.0)

    attributions = JSONField(default=dict)

    datasource = models.ForeignKey(DataSource, blank=False, on_delete=models.CASCADE)
    categories = ArrayField(JSONField(default=dict), default=list)

    productbase = models.ForeignKey(RetailProduct, blank=True, null=True, on_delete=models.CASCADE)


class OrderRow(RawModel):
    refound = models.BooleanField(default=False)

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    datasource = models.ForeignKey(DataSource, blank=False, on_delete=models.CASCADE)
    attributions = JSONField(default=dict)
    sale_price = models.FloatField(default=0.0)
    quantity = models.IntegerField(default=1)
    total_price = models.FloatField(default=0.0)
    productbase_id = models.IntegerField(default=1)


class Order(RawModel):
    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    external_id = models.TextField(blank=False)
    clientbase_id = models.IntegerField(blank=True, null=True)
    status = models.CharField(max_length=64, default=str)
    is_transaction = models.BooleanField(default=True)
    brand_id = models.CharField(max_length=64, default=str)
    total_price = models.FloatField(default=0.0)
    datetime = models.DateTimeField(blank=True, null=True)

    purchasebase = models.ForeignKey(PurchaseBase, null=True, on_delete=models.CASCADE)
    datasource = models.ForeignKey(DataSource, blank=False, on_delete=models.CASCADE)
    attributions = JSONField(default=dict)
