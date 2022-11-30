import datetime
import itertools
import statistics
from dateutil.relativedelta import relativedelta

from django.utils import timezone
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast
from django.db.models import Count, Func, Max, Min, IntegerField
from django.db.models.expressions import OuterRef, Subquery

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart, HorizontalBarChart, DataCard

from filtration.conditions import DateRangeCondition, ModeCondition
from orderly_core.team.charts import client_behavior_charts
from cerem.tasks import clickhouse_client
from cerem.utils import F
from orderly_core.team.charts import overview_charts, AttributionPieChart, past_charts
from charts.drawers import MatrixChart
from charts.registries import chart_category, dashboard_preset
from .models import EventBase, LevelLogBase, EventLogBase, EventBase, MemberLevelBase


@overview_charts.chart(name='等級人數圓餅圖')
class MemberLevelPieChart(PieChart):
    def draw(self):
        now = timezone.now()
        clients = self.team.clientbase_set.filter(removed=False)
        if not clients.exists():
            raise NoData('資料不足')
        client_qs = clients.annotate(
            current_level_name=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=now).order_by('-from_datetime').values('to_level__name')[:1]
            )
        )
        client_qs = client_qs.filter(current_level_name__isnull=False).values('current_level_name').annotate(count=Count('id'))
        for name, count in client_qs.values_list('current_level_name', 'count'):
            self.create_label(name=name, data=count,notes={'tooltip_value': '{data} 人'})

@overview_charts.chart(name='等級升降續熱區圖')
class LevelUpMatrMatrix(MatrixChart):
    def draw(self):
        levels = MemberLevelBase.objects.filter(removed=False).order_by('rank').values_list('id', 'name')
        print('levels: ', levels)
        data = [
            {'x': '初始', 'y': '一般會員', 'v': 0},
            {'x': '升等', 'y': '一般會員', 'v': 0.2},
            {'x': '初始', 'y': '銅等會員', 'v': 0.6},
            {'x': '升等', 'y': '銅等會員', 'v': 1.0},
            {'x': '降等', 'y': '一般會員', 'v': 0.2},
            {'x': '降等', 'y': '銅等會員', 'v': 0.6},
            {'x': '降等', 'y': '銀等會員', 'v': 1.0},
            {'x': '初始', 'y': '銀等會員', 'v': 0.2},
            {'x': '升等', 'y': '銀等會員', 'v': 0.4},
            {'x': '續等', 'y': '一般會員', 'v': 0.6},
            {'x': '續等', 'y': '銅等會員', 'v': 1.0},
            {'x': '續等', 'y': '銀等會員', 'v': 0.2},
            {'x': '降等', 'y': '金等會員', 'v': 1.0},
            {'x': '初始', 'y': '金等會員', 'v': 0.2},
            {'x': '升等', 'y': '金等會員', 'v': 0.4},
            {'x': '續等', 'y': '金等會員', 'v': 0.6},
        ]
        for item in data:
            self.set_value(item['x'], item['y'], item['v'])


@overview_charts.chart(name='等級即將到期直條圖')
class FutureLevelDue(BarChart):
    def draw(self):
        now = timezone.now()
        client_qs = self.team.clientbase_set.filter(removed=False)
        date_start = now.replace(day=1, hour=0, minute=0, second=0)

        client_qs = client_qs.annotate(
            current_level_due=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=now).order_by('-from_datetime').values('to_datetime')[:1]
            ),
            current_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=now).order_by('-from_datetime').values('to_level_id')[:1]
            )
        )
        if self.options.get('table_mode'):
            levels = MemberLevelBase.objects.filter(removed=False).values_list('id', 'name')
            for level_id, name in levels:
                labels = []
                data = []
                for i in range(12):
                    month_start = date_start + relativedelta(months=i)
                    month_end = month_start + relativedelta(months=1)
                    labels.append(month_start.strftime('%Y/%m'))
                    clients = client_qs.filter(current_level_id=level_id, current_level_due__range=[month_start, month_end])
                    data.append(clients.count())

                self.set_labels(labels)
                notes = {
                    'tooltip_value': '等級即將到期數 <br>{data} 人',
                    'tooltip_name': ' '
                }

                self.create_label(name=name, data=data, notes=notes)

        else:
            labels = []
            data = []
            for i in range(12):
                month_start = date_start + relativedelta(months=i)
                month_end = month_start + relativedelta(months=1)
                labels.append(month_start.strftime('%Y/%m'))
                clients = client_qs.filter(current_level_due__range=[month_start, month_end])
                data.append(clients.count())

            self.set_labels(labels)
            notes = {
                'tooltip_value': '等級即將到期數 <br>{data} 人',
                'tooltip_name': ' '
            }

            self.create_label(name='人數', data=data, notes=notes)


@past_charts.chart(name='等級人數往期直條圖')
class LevelClientCountTracing(BarChart):
    '''
    Hidden options:
        -trace_days:
            format: []
            default: [365, 30, 7, 1]
            explain: determine datetime points of x-axis.
    '''
    def __init__(self):
        super().__init__()
        self.trace_days = [365, 30, 7, 1]

    def get_labels(self):
        labels = []
        for days in self.trace_days:
            labels.append(f'{days} 天前')
        return labels

    def get_labels_info(self):
        labels = []
        for days in self.trace_days:
            now = timezone.now()
            date_string = (now - datetime.timedelta(days=days)).strftime('%Y 年 %m 月 %d 日')
            labels.append(f'{date_string} ~ {date_string}')
        return labels

    def draw(self):
        self.trace_days = self.options.get('trace_days', self.trace_days)
        now = timezone.now()
        client_qs = self.team.clientbase_set.filter(removed=False)
        self.set_total(len(client_qs))
        levels = MemberLevelBase.objects.filter(removed=False).order_by('rank').values_list('id', 'name')
        for level_id, name in levels:
            data = []
            for days in self.trace_days:
                date = now - datetime.timedelta(days=days)
                qs = client_qs.annotate(
                    current_level_id=Subquery(
                        LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=date).order_by('-from_datetime').values('to_level_id')[:1]
                    )
                )
                clients = qs.filter(current_level_id=level_id)
                data.append(clients.count())
            notes = {
                'tooltip_value': f'{name}會員人數<br>{{data}} 人',
                'tooltip_name': ' '
            }

            self.create_label(name=name, data=data, notes=notes)

@dashboard_preset
class Levels:
    name = '等級模組'
    charts = [
        MemberLevelPieChart.preset('等級人數圓餅圖'),
        # LevelUpMatrMatrix.preset('等級升降熱區圖'),
        FutureLevelDue.preset('等級即將到期直條圖', width='full'),
        LevelClientCountTracing.preset('等級人數往期直條圖')
    ]
