from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.postgres.fields import JSONField
from django.utils import timezone

from core.utils import ReadableJSONFormField

from plan.models import PlanSetting
from ..retail.models import RepurchaseCycle, ProductCategory, RetailProduct, PurchaseBase, OrderProduct


from .models import (
    Brand, MemberLevelBase, LevelLogBase, BrandAuth, PointLogBase
)


@admin.register(MemberLevelBase)
class MemberLevelBaseAdmin(admin.ModelAdmin):

    list_display = (
        'id',
        'c_at',
        'name',
        'rank',
        'external_id',
    )

    search_fields = ('name', 'external_id')


@admin.register(Brand)
class BrandAdmin(admin.ModelAdmin):

    list_display = (
        'id',
        'c_at',
        'name',
        'order',
        'external_id',
    )

    search_fields = ('name', 'external_id')

@admin.register(OrderProduct)
class RetailProductAdmin(admin.ModelAdmin):

    list_display = (
        'id',
        'c_at',
        'team',
        'datalist_id',
        'productbase',
        'purchasebase',
        'clientbase',
        'refound',
        'sale_price',
        'quantity',
        'total_price'
    )

    search_fields = ('name', 'external_id')

@admin.register(RetailProduct)
class RetailProductAdmin(admin.ModelAdmin):

    list_display = (
        'id',
        'c_at',
        'name',
        'price',
        'external_id',
        'attributions'
    )

    search_fields = ('name', 'external_id')

admin.site.register(LevelLogBase)
admin.site.register(RepurchaseCycle)
admin.site.register(ProductCategory)
# admin.site.register(RetailProduct)
admin.site.register(PurchaseBase)
#admin.site.register(OrderProduct)
admin.site.register(BrandAuth)
admin.site.register(PointLogBase)
