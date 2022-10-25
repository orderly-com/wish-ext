import datetime
import urllib

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.views.generic import ListView, View, TemplateView, UpdateView, CreateView
from django.shortcuts import get_object_or_404, redirect, reverse
from django.db.models import Count, Max, Q, F, Sum, Value
from django.http import JsonResponse, HttpResponseForbidden
from django.utils import timezone, translation
from django.conf import settings

from core import views as core
from core.utils import TeamAuthPermission, ForestTimer, array_to_dict, make_datetimeStart_datetimeEnd, querydict_to_dict, sort_list_by_key, str_to_hex, bulk_create, bulk_update

from team.views import TeamMixin

from .models import AbstractProduct

from ..extension import retail_ext

retail_router = retail_ext.router('retail/', name='retail')
