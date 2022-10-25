from django.contrib import admin
from django.contrib.admin import SimpleListFilter
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.postgres.fields import JSONField
from django.utils import timezone

from core.utils import ReadableJSONFormField

from plan.models import PlanSetting

from .models import (
    Brand, MemberLevelBase
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
