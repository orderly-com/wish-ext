import datetime
import itertools
import statistics
from dateutil.relativedelta import relativedelta

from django.utils import timezone
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast
from django.db.models import Count, Func, Max, Min, IntegerField, Sum
from django.db.models.expressions import OuterRef, Subquery

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart, HorizontalBarChart, DataCard

from filtration.conditions import DateRangeCondition, ModeCondition
from orderly_core.team.charts import client_behavior_charts
from cerem.tasks import clickhouse_client
from cerem.utils import F
from orderly_core.team.charts import overview_charts, AttributionPieChart, past_charts, trend_charts
from charts.drawers import MatrixChart
from charts.registries import chart_category, dashboard_preset
from .models import EventBase, LevelLogBase, EventLogBase, EventBase, MemberLevelBase
from wish_ext.retail.models import PurchaseBase


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
    def __init__(self):
        super().__init__()
        self.add_options(join_time_range=DateRangeCondition('時間範圍'))

    def get_level_rank(self, client_data:list, level_data:dict):
        for index, per_cl_data in enumerate(client_data):
            current_level_id = per_cl_data.get('current_level_id','')
            previous_level_id = per_cl_data.get('previous_level_id','')
            if current_level_id:
                if current_level_id == level_data[current_level_id]['id']:
                    client_data[index]['current_level_name'] = level_data[current_level_id]['name']
                    client_data[index]['current_rank'] = level_data[current_level_id]['rank']

            if previous_level_id:
                if previous_level_id == level_data[previous_level_id]['id']:
                    client_data[index]['previous_level_name'] = level_data[previous_level_id]['name']
                    client_data[index]['previous_rank'] = level_data[previous_level_id]['rank']

        return client_data

    def level_data_dict(self, level_data:list):
        data = {}
        for per_data in level_data:
            data[per_data['id']] = per_data
        return data

    def level_eval(self, data:list):
        res_data = []
        for per_data in data:
            current_rank = per_data.get('current_rank')
            previous_rank = per_data.get('previous_rank')
            if current_rank is None and previous_rank is None:
                per_data['x'] = '無資料'
                continue
            if current_rank and not previous_rank:
                per_data['x'] = '初始'
            if current_rank is not None and previous_rank:
                if current_rank == previous_rank:
                    per_data['x'] = '續等'
                elif current_rank > previous_rank:
                    per_data['x'] = '升等'
                elif current_rank < previous_rank:
                    per_data['x'] = '降等'
            res_data.append(per_data)
        return res_data

    def data_assign(self, data: list, level_data):
        format_data = {}
        for level_id, level_per_data in level_data.items():
            for level_direction in ['初始', '升等', '降等', '續等']:
                key_string = level_direction + '__' + level_per_data['name']
                format_data[key_string] = 0
        # format_data: {'初始__一般會員': 0, '升等__一般會員': 0, '降等__一般會員': 0 ...}
        for per_data in data:
            per_level = per_data.get('x', '')
            per_current_level_name = per_data.get('current_level_name', '')
            if per_level and per_current_level_name:
                key_string = per_level + '__' + per_current_level_name
                format_data[key_string] = format_data.get(key_string, 0) + 1

        return format_data


    def draw(self):
        now = timezone.now()
        clients = self.team.clientbase_set.filter(removed=False)
        if not clients.exists():
            raise NoData('資料不足')

        date_start, date_end = self.get_date_range('join_time_range', now - datetime.timedelta(days=365), now)

        client_qs = clients.annotate(
            current_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__gte=date_start, from_datetime__lte=date_end).order_by('-from_datetime').values('to_level__id')[:1]
            ),
            previous_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__gte=date_start, from_datetime__lte=date_end).order_by('-from_datetime').values('from_level__id')[:1]
            )
        )
        level_data = list(MemberLevelBase.objects.filter(removed=False).values())
        level_data = self.level_data_dict(level_data)
        client_data = self.get_level_rank(list(client_qs.values()), level_data)
        f_data = self.level_eval(client_data)
        f_data = self.data_assign(f_data, level_data)
        for key_string, count in f_data.items():
            key_string_list = key_string.split('__')
            x = key_string_list[0]
            y = ''.join(key_string_list[1:])
            self.set_value(x, y, count)


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

@trend_charts.chart(name='等級人數折線圖')
class MemberLevelTrend(LineChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=365)).isoformat(),
                now.isoformat()
            )
        ))
    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def draw(self):
        client_qs = self.team.clientbase_set.filter(removed=False)
        date_start, date_end = self.get_date_range('time_range')
        date_list = self.get_per_date_list(date_start, date_end)
        self.set_date_range(date_start, date_end)
        self.set_total(len(client_qs))
        levels = MemberLevelBase.objects.filter(removed=False).order_by('rank').values_list('id', 'name')
        for level_id, name in levels:
            data = []
            for date in date_list:
                qs = client_qs.annotate(
                    current_level_id=Subquery(
                        LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=date).order_by('-from_datetime').values('to_level_id')[:1]
                    )
                )
                clients = qs.filter(current_level_id=level_id)
                data.append(clients.count())

            self.notes.update({
                'tooltip_value': f'{name}會員人數<br>{{data}} 人',
                'tooltip_name': ' '
            })

            self.create_label(name=name, data=data, notes=self.notes)

@trend_charts.chart(name='等級即將到期趨勢折線圖')
class FutureLevelDueTrend(LineChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(
            time_range=DateRangeCondition('時間範圍')
            .config(max_date=(now + datetime.timedelta(days=365)).isoformat())
            .default(
                (
                    now.isoformat(),
                    (now + datetime.timedelta(days=365)).isoformat()
                )
            )
        )
    def get_month_count(self, date_start, date_end):
        year_begin, year_end = date_start.year, date_end.year
        month_begin, month_end = date_start.month, date_end.month
        if year_begin == year_end:
            months = month_end - month_begin
        else:
            months = (year_end - year_begin) * 12 + month_end - month_begin

        return months

    def draw(self):
        now = timezone.now()
        client_qs = self.team.clientbase_set.filter(removed=False)
        date_start, date_end = self.get_date_range('time_range')

        client_qs = client_qs.annotate(
            current_level_due=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__gte=date_start, from_datetime__lte=date_end).order_by('-from_datetime').values('to_datetime')[:1]
            ),
            current_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__gte=date_start, from_datetime__lte=date_end).order_by('-from_datetime').values('to_level_id')[:1]
            )
        )

        levels = MemberLevelBase.objects.filter(removed=False).values_list('id', 'name')
        for level_id, name in levels:
            labels = []
            data = []
            month_count = self.get_month_count(date_start, date_end)
            for i in range(month_count+1):
                month_start = date_start + relativedelta(months=i)
                month_end = month_start + relativedelta(months=1)
                labels.append(month_start.strftime('%Y/%m'))
                clients = client_qs.filter(current_level_id=level_id, current_level_due__range=[month_start, month_end])
                data.append(clients.count())
            self.set_labels(labels)
            notes = {
                'tooltip_value': '{name}: {data} 人',
                'tooltip_name': ' '
            }

            self.create_label(name=name, data=data, notes=notes)
@past_charts.chart(name='交易人數往期直條圖')
class PurchaseMemberCount(BarChart):
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
        data = []
        now = timezone.now()
        for days in self.trace_days:
            date = now - datetime.timedelta(days=days)
            # distinct data base on clientbase_id
            clients = PurchaseBase.objects.filter(removed=False).values('clientbase_id').distinct().filter(datetime__lt=date)
            data.append(clients.count())
        notes = {
            'tooltip_value': f'{{data}} 人'
        }

        self.create_label(name=' ', data=data, notes=notes)


@past_charts.chart(name='交易金額往期直條圖')
class PurchaseNumberCount(BarChart):
    ACCUMULATED = 'all'
    ADDED = 'member'
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
        self.add_options(
            #join_time_range=DateRangeCondition('時間範圍'),
            mode=ModeCondition('').choice(
                {'text': '營業額', 'id': self.ADDED},
                {'text': '客單價', 'id': self.ACCUMULATED},
                {'text': '平均金額', 'id': self.ACCUMULATED}
            ).default(self.ACCUMULATED)
        )

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
        data = []
        now = timezone.now()
        # purchase_base_set = PurchaseBase.objects.filter(removed=False)
        #total_price = purchase_base_set.aggregate(Sum('total_price'))
        #print('total_price: ', total_price)
        #self.set_total(total_price['total_price__sum'])
        for days in self.trace_days:
            date = now - datetime.timedelta(days=days)
            purchase_base_set = PurchaseBase.filter(removed=False).filter(datetime__lt=date)
            prices = purchase_base_set.values()
            data.append(prices)
        notes = {
            'tooltip_value': f'{{data}} 人'
        }

        self.create_label(name=' ', data=data, notes=notes)

@past_charts.chart(name='交易單數往期直條圖')
class PurchaseOrderCount(BarChart):
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
        data = []
        now = timezone.now()
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        self.set_total(len(purchase_base_set))
        for days in self.trace_days:
            date = now - datetime.timedelta(days=days)
            purchase_base = purchase_base_set.filter(datetime__lt=date)
            data.append(purchase_base.count())
        notes = {
            'tooltip_value': f'{{data}} 人'
        }

        self.create_label(name=' ', data=data, notes=notes)




@dashboard_preset
class Levels:
    name = '等級模組'
    charts = [
        MemberLevelPieChart.preset('等級人數圓餅圖'),
        #LevelUpMatrMatrix.preset('等級升降續熱區圖'),
        FutureLevelDue.preset('等級即將到期直條圖', width='full'),
        LevelClientCountTracing.preset('等級人數往期直條圖'),
        MemberLevelTrend.preset('等級人數折線圖', width='full'),
        FutureLevelDueTrend.preset('等級即將到期趨勢折線圖', width='full')
    ]

@dashboard_preset
class Purchae:
    name = '交易模組'
    charts = [
        PurchaseMemberCount.preset('交易人數往期直條圖', chart_type='bar'),
        #PurchaseNumberCount.preset('交易金額往期直條圖', chart_type='bar'),
        PurchaseOrderCount.preset('交易單數往期直條圖', chart_type='bar'),

    ]