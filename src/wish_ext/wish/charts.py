from collections import defaultdict
import datetime
import itertools
import statistics
from dateutil.relativedelta import relativedelta
import math
import numpy as np

from django.utils import timezone
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast, ExtractWeekDay
from django.db.models import Count, Func, Max, Min, IntegerField, Sum, Avg
from django.db.models.expressions import OuterRef, Subquery
from dateutil import rrule

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart, HorizontalBarChart, DataCard, TextArea

from filtration.conditions import DateRangeCondition, ModeCondition, SingleSelectCondition, ChoiceCondition, SelectCondition, Condition, DropDownCondition
from orderly_core.team.charts import client_behavior_charts
from cerem.tasks import clickhouse_client
from cerem.utils import F
from orderly_core.team.charts import overview_charts, AttributionPieChart, past_charts, trend_charts
from charts.drawers import MatrixChart, HeatMapChart
from charts.registries import chart_category, dashboard_preset
from .models import EventBase, LevelLogBase, EventLogBase, EventBase, MemberLevelBase
from wish_ext.retail.models import PurchaseBase
from wish_ext.wish.models import Brand, BrandAuth
import pandas as pd


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
                (now - datetime.timedelta(days=30)).isoformat(),
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
                    (now + datetime.timedelta(days=90)).isoformat()
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

@overview_charts.chart(name='會員交易指標')
class PurchaseIndicators(TextArea):
    def draw(self):
        self.set_data('12345')

@overview_charts.chart(name='營業額')
class TurnOverCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        qs_result_dict =  purchase_base_set.aggregate(Sum('total_price'))
        turnover = qs_result_dict.get('total_price__sum', 0)
        if turnover is None:
            turnover = 0
        self.set_data(math.ceil(turnover), postfix='元')

@overview_charts.chart(name='會員交易人數')
class PurchaseMemberCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        member_count = purchase_base_set.values('clientbase_id').distinct().count()
        self.set_data(member_count, postfix='人')

@overview_charts.chart(name='會員交易率')
class PurchaseMemberRateCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        purchase_member_count = purchase_base_set.values('clientbase_id').distinct().count()
        clients_count = self.team.clientbase_set.filter(removed=False).count()
        result = (purchase_member_count / clients_count) * 100
        self.set_data('%.1f'%result, postfix='%')

@overview_charts.chart(name='交易筆數')
class PurchaseCountCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        purchase_count = purchase_base_set.count()
        self.set_data(purchase_count, postfix='筆')

@overview_charts.chart(name='平均金額')
class AvgPriceCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        qs_result_dict =  purchase_base_set.aggregate(Sum('total_price'))
        turnover = qs_result_dict.get('total_price__sum', 0)
        if turnover is None:
            turnover = 0
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        purchase_count = purchase_base_set.count()
        if not turnover:
            result = 0
        else:
            result = math.ceil(turnover / purchase_count)
        self.set_data(result, postfix='元')

@overview_charts.chart(name='客單價')
class AvgPerMemberCard(DataCard):
    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        qs_result_dict =  purchase_base_set.aggregate(Sum('total_price'))
        turnover = qs_result_dict.get('total_price__sum', 0)
        if turnover is None:
            turnover = 0
        member_count = purchase_base_set.values('clientbase_id').distinct().count()
        if not turnover:
            result = 0
        else:
            result = math.ceil(turnover / member_count)
        self.set_data(result, postfix='元')
@overview_charts.chart(name='交易時間熱區圖')
class PurchaseTimeHeatMap(HeatMapChart):
    TURNOVER = 'turnover'
    PERCUSPRICE = 'per_cus_price'
    AVGPRICE = 'avg_price'
    MEMBER_COUNT = 'member_count'
    ORDER_COUNT = 'order_count'
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.x_values = ['01:00', '02:00', '03:00', '04:00','05:00','06:00', '07:00', '08:00', '09:00', '10:00', \
            '11:00', '12:00','13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', \
                '21:00','22:00', '23:00', '24:00']
        # self.y_values = ['週一', '週二', '週三', '週四', '週五', '週六', '週日']
        self.y_values = ['週日', '週六', '週五', '週四', '週三', '週二', '週一']
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )),
            data_options=DropDownCondition('').choice(
                {'id': self.PERCUSPRICE, 'text': '客單價'},
                {'id': self.TURNOVER, 'text': '營業額'},
                {'id': self.AVGPRICE, 'text': '平均金額'},
                {'id': self.MEMBER_COUNT, 'text': '人數'},
                {'id': self.ORDER_COUNT, 'text': '交易單數'}
            ).default(self.PERCUSPRICE)
        )

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_x(self):
        return '最後一次消費天數'

    def explain_y(self):
        return '消費頻率'

    def get_x_value(self, hour):
        if hour < 10:
            return f'0{hour}:00'
        return f'{hour}:00'

    def get_y_value(self, day):
        # django weekday
        weekdays_map = {2:'週一', 3:'週二', 4:'週三', 5:'週四', 6:'週五', 7:'週六', 1:'週日'}
        return weekdays_map[day]

    def data_router(self, query_set, data_option):
        '''
        客單價
        營業額
        平均金額
        人數
        交易單數
        '''
        if data_option == self.TURNOVER:
            return self.get_turnover_data(query_set)
        elif data_option == self.AVGPRICE:
            return self.get_avg_price_data(query_set)
        elif data_option == self.PERCUSPRICE:
            return self.get_per_cus_price_data(query_set)
        elif data_option == self.MEMBER_COUNT:
            return self.get_member_count_data(query_set)
        elif data_option == self.ORDER_COUNT:
            return self.get_order_count_data(query_set)

    def get_turnover_data(self, qs):
        return qs.annotate(weekday=ExtractWeekDay('datetime')).values('datetime__hour', 'weekday').annotate(value=Sum('total_price'))

    def get_order_count_data(self, qs):
        return qs.annotate(weekday=ExtractWeekDay('datetime')).values('datetime__hour', 'weekday').annotate(value=Count('id'))

    def get_avg_price_data(self, qs):
        return qs.annotate(weekday=ExtractWeekDay('datetime')).values('datetime__hour', 'weekday').annotate(value=Avg('total_price'))

    def get_per_cus_price_data(self, qs):
        return qs.annotate(weekday=ExtractWeekDay('datetime')).values('datetime__hour', 'weekday', 'clientbase_id').annotate(value=Avg('total_price'))

    def get_member_count_data(self, qs):
        return qs.annotate(weekday=ExtractWeekDay('datetime')).values('datetime__hour', 'weekday').annotate(value=Count('clientbase_id'))

    def result_map(self, data_option):
        if data_option == self.TURNOVER:
            return 'price_sum'
        elif data_option == self.AVGPRICE:
            return 'avg_price'
        elif data_option == self.PERCUSPRICE:
            return 'per_cus_price'
        elif data_option == self.MEMBER_COUNT:
            return 'member_count'
        elif data_option == self.ORDER_COUNT:
            return 'member_count'


    def draw(self):
        data_options = self.options.get('data_options','')
        date_start, date_end = self.get_date_range('time_range')
        purchasebase_qs = PurchaseBase.objects.filter(removed=False).filter(datetime__gte=date_start, datetime__lte=date_end)
        if data_options:
            purchasebase_qs = self.data_router(purchasebase_qs, data_options)
        f_data = {}
        for x in self.x_values:
            for y in self.y_values:
                key_string = x + '__' + y
                f_data[key_string] = 0

        for per_data in purchasebase_qs:
            x_value = self.get_x_value(per_data['datetime__hour'])
            y_value = self.get_y_value(per_data['weekday'])
            key_string = x_value + '__' + y_value
            f_data[key_string] += per_data['value']

        for per_data in f_data:
            x = per_data.split('__')[0]
            y = ''.join(per_data.split('__')[1:])
            self.set_value(x, y, f_data[per_data])



@overview_charts.chart(name='交易客單價區間單數直條圖')
class AvgPerMemberRange(BarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(
            time_range=DateRangeCondition('時間範圍')
            .config(max_date=(now + datetime.timedelta(days=365)).isoformat())
            .default(
                (
                    now.isoformat(),
                    (now + datetime.timedelta(days=90)).isoformat()
                )
            )
        )

    def explain_y(self):
        return '單數'

    def get_labels(self):
        step = self.options.get('step', 999)
        minimum = self.options.get('min', 999)
        maximum = self.options.get('max', 20000)

        labels = []
        labels.append(f'<= {minimum}')
        while minimum < maximum and minimum != (maximum - 1):
            minimum += 1
            labels.append(f'{minimum} - {minimum + step}')
            minimum += step

        labels.append(f'>= {maximum}')
        return labels

    def draw(self):
        now = timezone.now()
        step = self.options.get('step', 999)
        minimum = self.options.get('min', 999)
        maximum = self.options.get('max', 20000)

        labels = []
        tooltip_titles = []
        labels.append(f'<= {minimum}')
        tooltip_titles.append(f'<= {minimum} 單價區間')
        while minimum < maximum and minimum != (maximum - 1):
            minimum += 1
            labels.append(f'{minimum} - {minimum + step}')
            tooltip_titles.append(f'{minimum} - {minimum + step} 單價區間')
            minimum += step

        labels.append(f'>= {maximum}')
        tooltip_titles.append(f'>= {maximum}')
        self.set_labels(labels)

        def get_bin_index(price):
            if price < 0:
                return 0
            if price >= maximum:
                return -1
            index = int((price - minimum) / step)

            return index
        date_start, date_end = self.get_date_range('time_range')

        purchase_set = PurchaseBase.objects.filter(removed=False).filter(datetime__gte=date_start, datetime__lte=date_end)
        if not purchase_set.exists():
            raise NoData('尚無資料')
        self.set_total(len(purchase_set))
        step = self.options.get('step', 999)
        minimum = self.options.get('min', 999)
        maximum = self.options.get('max', 20000)
        data = [0] * int((maximum-minimum) / step + 1)
        per_purchase_set = purchase_set.all().values('total_price').annotate(count=Count('id'))
        for per_data in per_purchase_set:
            index = get_bin_index(per_data['total_price'])
            data[index] += per_data['count']

        notes = {
            'tooltip_title': tooltip_titles,
            'tooltip_name': ' ',
            'tooltip_value': '{data} 筆單',
        }
        self.create_label(name='', data=data, notes=notes)

@overview_charts.chart(name='交易金額直條圖(集團)')
class PurchaseNumberBar(BarChart):
    TURNOVER = 'turnover'
    PERCUSPRICE = 'per_cus_price'
    AVGPRICE = 'avg_price'
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )),
            select_option=ModeCondition('').choice(
                {'id': self.TURNOVER, 'text': '營業額'},
                {'id': self.PERCUSPRICE, 'text': '客單價'},
                {'id': self.AVGPRICE, 'text': '平均金額'},
            ).default(self.TURNOVER)
        )


    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('全部品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()

        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])

        self.add_options(all_brand=brand_selection)

    def explain_y(self):
        return '金額'

    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def get_turnover_data(self, query_set, date):
        qs_result_dict =  query_set.filter(datetime__year=date.year,datetime__month=date.month).aggregate(Sum('total_price'))
        result = 0
        if qs_result_dict.get('total_price__sum'):
            result = qs_result_dict.get('total_price__sum')
        return result

    def get_avg_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        order_count = query_set.filter(datetime__year=date.year,datetime__month=date.month).count()
        if order_count:
            return math.ceil(turn_over / order_count)
        else:
            return 0

    def get_per_cus_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        member_count = query_set.values('clientbase_id').filter(removed=False).distinct().filter(datetime__year=date.year,datetime__month=date.month).count()
        if member_count:
            return math.ceil(turn_over / member_count)
        else:
            return 0

    def get_data_router(self, option, query_set, date):
        if option == self.TURNOVER:
            return self.get_turnover_data(query_set, date)
        elif option == self.AVGPRICE:
            return self.get_avg_price_data(query_set, date)
        elif option == self.PERCUSPRICE:
            return self.get_per_cus_price_data(query_set, date)

    def explain_y(self):
        return '金額'


    def draw(self):
        # brand option selection
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        date_start, date_end = self.get_date_range('time_range')
        if select_brand_id is None:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        elif select_brand_id != 'all':
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id).filter(datetime__lte=date_end, datetime__gte=date_start)
        else:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        months_difference = rrule.rrule(rrule.MONTHLY, dtstart = date_start, until = date_end).count()
        labels = []
        dates_list = []
        for diff_count in range(months_difference):
            month_start = date_start + relativedelta(months=diff_count)
            month_end = month_start + relativedelta(months=1)
            dates_list.append(month_start)
            labels.append(month_start.strftime('%Y/%m'))
        # price count option
        months_difference = rrule.rrule(rrule.MONTHLY, dtstart = date_start, until = date_end).count()
        labels = []
        dates_list = []
        for diff_count in range(months_difference):
            month_start = date_start + relativedelta(months=diff_count)
            month_end = month_start + relativedelta(months=1)
            dates_list.append(month_start)
            labels.append(month_start.strftime('%Y/%m'))
        self.set_labels(labels)
        self.set_total(len(purchase_base_set))
        select_option = self.options.get('select_option','')
        data = []
        now = timezone.now()
        for date in dates_list:
            result = self.get_data_router(select_option, purchase_base_set, date)
            data.append(result)
        self.notes.update({
                'tooltip_value': '交易金額 {data} 元',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)
@overview_charts.chart(name='交易人數直條圖(集團)')
class PurchaseMemCountBar(BarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))
    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()


        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])


        self.add_options(all_brand=brand_selection)


    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '人數'

    def draw(self):
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        date_start, date_end = self.get_date_range('time_range')
        if select_brand_id is None:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        elif select_brand_id != 'all':
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id).filter(datetime__lte=date_end, datetime__gte=date_start)
        else:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        months_difference = rrule.rrule(rrule.MONTHLY, dtstart = date_start, until = date_end).count()
        labels = []
        dates_list = []
        for diff_count in range(months_difference):
            month_start = date_start + relativedelta(months=diff_count)
            month_end = month_start + relativedelta(months=1)
            dates_list.append(month_start)
            labels.append(month_start.strftime('%Y/%m'))
        self.set_labels(labels)
        self.set_total(len(purchase_base_set))
        data = []
        now = timezone.now()
        for date in dates_list:
            order_member = purchase_base_set.filter(removed=False).values('clientbase_id').distinct().filter(datetime__year=date.year,datetime__month=date.month)
            data.append(order_member.count())
        self.notes.update({
                'tooltip_value': '交易人數 {data} 人',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)


@overview_charts.chart(name='交易單數直條圖(集團)')
class PurchaseOrderBar(BarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))
    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()


        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])

        self.add_options(all_brand=brand_selection)


    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '單數'

    def draw(self):
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        date_start, date_end = self.get_date_range('time_range')
        if select_brand_id is None:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        elif select_brand_id != 'all':
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id).filter(datetime__lte=date_end, datetime__gte=date_start)
        else:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands).filter(datetime__lte=date_end, datetime__gte=date_start)
        months_difference = rrule.rrule(rrule.MONTHLY, dtstart = date_start, until = date_end).count()
        labels = []
        dates_list = []
        for diff_count in range(months_difference):
            month_start = date_start + relativedelta(months=diff_count)
            month_end = month_start + relativedelta(months=1)
            dates_list.append(month_start)
            labels.append(month_start.strftime('%Y/%m'))
        self.set_labels(labels)
        self.set_total(len(purchase_base_set))
        data = []
        now = timezone.now()
        for date in dates_list:
            order = purchase_base_set.filter(removed=False).filter(datetime__year=date.year,datetime__month=date.month)
            data.append(order.count())
        self.notes.update({
                'tooltip_value': '交易單數 {data} 單',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)

@overview_charts.chart(name='RF分析')
class RFHeatMap(MatrixChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_x(self):
        return '最後一次消費天數'

    def explain_y(self):
        return '消費頻率'

    def get_x_value(self, day):
        x_values = ['0-7天', '8-15天', '16-22天', '23-30天','31-90天','>90天']
        for index, day_range_str in enumerate(x_values):
            if index != len(x_values) -1:
                range_list = day_range_str.replace('天','').split('-')
                range_list = [int(day_string) for day_string in range_list]
                if day > range_list[0] and day < range_list[1]:
                    return x_values[index]
        return x_values[index]

    def get_y_value(self, count):
        y_values = ['1', '2', '3', '4', '5', '>6']
        for index, count_string in enumerate(y_values):
            if index != len(y_values) - 1:
                if count == int(count_string):
                    return y_values[index]
        return y_values[index]

    def draw(self):
        date_start, date_end = self.get_date_range('time_range')
        purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(datetime__lte=date_end, datetime__gte=date_start)
        now = timezone.now()
        x_values = ['0-7天', '8-15天', '16-22天', '23-30天','31-90天','>90天']
        y_values = ['1', '2', '3', '4', '5', '>6']
        now = timezone.now()
        per_data_count = {}
        perchase_data = purchase_base_set.values('clientbase_id','datetime')
        for per_data in perchase_data:
            if per_data_count.get(per_data['clientbase_id']):
                # judge condition
                per_data_count[per_data['clientbase_id']]['count'] += 1
                if per_data['datetime'] < per_data_count[per_data['clientbase_id']]['last_datetime']:
                    per_data_count[per_data['clientbase_id']]['last_datetime'] = per_data['datetime']
                    per_data_count[per_data['clientbase_id']]['last_purchase_day'] = (now - per_data['datetime']).days
            else:
                per_data_count[per_data['clientbase_id']] = {
                    'count': 1,
                    'last_datetime': per_data['datetime'],
                    'last_purchase_day': (now - per_data['datetime']).days
                }
        f_data = {}

        for x in x_values:
            for y in y_values:
                key_string = x + '__' + y
                f_data[key_string] = 0

        for per_data in per_data_count:
            x_value = self.get_x_value(per_data_count[per_data]['last_purchase_day'])
            y_value = self.get_y_value(per_data_count[per_data]['count'])
            key_string = x_value + '__' + y_value
            if f_data.get(key_string):
                f_data[key_string] += 1
            else:
                f_data[key_string] = 1

        for per_data in f_data:
            x = per_data.split('__')[0]
            y = ''.join(per_data.split('__')[1:])
            self.set_value(x, y, f_data[per_data])

@overview_charts.chart(name='RFM分析')
class RFMHeatMap(MatrixChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_x(self):
        return '最後一次消費天數'

    def explain_y(self):
        return '消費頻率'

    def get_x_value(self, day):
        x_values = ['0-7天', '8-15天', '16-22天', '23-30天','31-90天','>90天']
        for index, day_range_str in enumerate(x_values):
            if index != len(x_values) -1:
                range_list = day_range_str.replace('天','').split('-')
                range_list = [int(day_string) for day_string in range_list]
                if day > range_list[0] and day < range_list[1]:
                    return x_values[index]
            else:
                return x_values[index]

    def get_y_value(self, count):
        y_values = ['1', '2', '3', '4', '5', '>6']
        for index, count_string in enumerate(y_values):
            if index != len(y_values) - 1:
                if count == int(count_string):
                    return y_values[index]
            else:
                return y_values[index]

    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        date_start, date_end = self.get_date_range('time_range')
        now = timezone.now()
        x_values = ['0-7天', '8-15天', '16-22天', '23-30天','31-90天','>90天']
        y_values = ['1', '2', '3', '4', '5', '>6']
        now = timezone.now()
        per_data_count = {}
        perchase_data = purchase_base_set.filter(datetime__lte=date_end, datetime__gte=date_start).values('clientbase_id','datetime','total_price')
        for per_data in perchase_data:
            if per_data_count.get(per_data['clientbase_id']):
                per_data_count[per_data['clientbase_id']]['count'] += 1
                per_data_count[per_data['clientbase_id']]['total_price'] += per_data['total_price']
                if per_data['datetime'] < per_data_count[per_data['clientbase_id']]['last_datetime']:
                    per_data_count[per_data['clientbase_id']]['last_datetime'] = per_data['datetime']
                    per_data_count[per_data['clientbase_id']]['last_purchase_day'] = (now - per_data['datetime']).days
            else:
                per_data_count[per_data['clientbase_id']] = {
                    'count': 1,
                    'total_price':  per_data['total_price'],
                    'last_datetime': per_data['datetime'],
                    'last_purchase_day': (now - per_data['datetime']).days
                }
        f_data = {}

        for x in x_values:
            for y in y_values:
                key_string = x + '__' + y
                f_data[key_string] = 0

        for per_data in per_data_count:
            x_value = self.get_x_value(per_data_count[per_data]['last_purchase_day'])
            y_value = self.get_y_value(per_data_count[per_data]['count'])
            avg = math.ceil(per_data_count[per_data]['total_price'] / per_data_count[per_data]['count'])
            key_string = x_value + '__' + y_value
            if f_data.get(key_string):
                f_data[key_string] += avg
            else:
                f_data[key_string] = avg

        for per_data in f_data:
            x = per_data.split('__')[0]
            y = ''.join(per_data.split('__')[1:])
            self.set_value(x, y, f_data[per_data])


@overview_charts.chart(name='RFM 人數直條圖')
class RFMCountBar(BarChart):
    def __init__(self):
        super().__init__()

    def explain_x(self):
        return '人數'

    def explain_y(self):
        return 'RFM分數'

    def get_turnover_data(self, query_set):
        qs_result_dict =  query_set.aggregate(Sum('total_price'))
        result = 0
        if qs_result_dict.get('total_price__sum'):
            result = qs_result_dict.get('total_price__sum')
        return result

    def get_avg_price_data(self, query_set):
        turn_over = self.get_turnover_data(query_set)
        order_count = query_set.count()
        if order_count:
            return math.ceil(turn_over / order_count)
        else:
            return 0

    def get_per_cus_price_data(self, query_set):
        turn_over = self.get_turnover_data(query_set)
        member_count = query_set.values('clientbase_id').filter(removed=False).distinct().count()
        if member_count:
            return math.ceil(turn_over / member_count)
        else:
            return 0

    def get_labels(self):
        return [num for num in range(1,16)]

    def draw(self):
        client_qs = self.team.clientbase_set.filter(removed=False)
        self.set_total(client_qs.count())
        rfm_total_qs = client_qs.all().values('id','rfm_total_score')
        data = [0] * 15
        labels = [num for num in range(1,16)]
        tooltip_title = []
        for label in labels:
            label_str = '分數 ' + f'{label}'
            tooltip_title.append(label_str)
        for rfm_data in rfm_total_qs:
            if rfm_data['rfm_total_score'] != 0:
                data[rfm_data['rfm_total_score']] += 1

        self.notes.update({
                'tooltip_title': tooltip_title,
                'tooltip_value': '{data} 人<br> 佔會員比例: {percentage}%',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)

@overview_charts.chart(name='交易回購人數直條圖')
class RepurchaseMemCountBar(BarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '人數'

    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        date_start, date_end = self.get_date_range('time_range')
        self.set_total(len(purchase_base_set))
        labels = ['首購', '第一次回購', '第二次回購', '第三次回購', '第四次回購', '大於四次回購']
        self.set_labels(labels)
        data = [0]*len(labels)
        now = timezone.now()
        dict_data = []
        value_data = []
        order_member_count = purchase_base_set.filter(datetime__lte=date_end, datetime__gte=date_start).values('clientbase_id').annotate(Count('clientbase_id'))
        if order_member_count:
            for count_data in order_member_count:
                dict_data.append(count_data['clientbase_id'])
                value_data.append(count_data['clientbase_id__count'])
                if count_data['clientbase_id__count'] == 1:
                    data[0] += 1
                elif count_data['clientbase_id__count'] == 2:
                    data[1] += 1
                elif count_data['clientbase_id__count'] == 3:
                    data[2] += 1
                elif count_data['clientbase_id__count'] == 4:
                    data[3] += 1
                elif count_data['clientbase_id__count'] == 5:
                    data[4] += 1
                else:
                    data[5] += 1

        self.notes.update({
                'tooltip_value': f'{{data}} 人<br> 佔會員比例: {{percentage}}%',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)

@overview_charts.chart(name='交易回購天數直條圖')
class RepurchaseDayCountBar(BarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))

    def get_labels(self):
        return['第一次回購', '第二次回購', '第三次回購', '第四次回購', '大於四次回購']

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '天數'

    def explain_x(self):
        return '回購頻率'

    def draw(self):
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        date_start, date_end = self.get_date_range('time_range')
        self.set_total(len(purchase_base_set))
        tooltip_titles = ['第一次回購', '第二次回購', '第三次回購', '第四次回購', '大於四次回購']
        day_data = [0]*len(tooltip_titles)
        data_member_count = [0] * len(tooltip_titles)
        now = timezone.now()
        per_data_count = {}
        perchase_data = purchase_base_set.filter(datetime__lte=date_end, datetime__gte=date_start).values('clientbase_id','datetime')
        for per_data in perchase_data:
            per_data_count[per_data['clientbase_id']] = per_data_count.get(per_data['clientbase_id'], 0) + 1
            if per_data_count.get(per_data['clientbase_id']) == 2:
                day_data[0] += (now - per_data['datetime']).days
                data_member_count[0] += 1
            elif per_data_count.get(per_data['clientbase_id']) == 3:
                day_data[1] += (now - per_data['datetime']).days
                data_member_count[1] += 1
            elif per_data_count.get(per_data['clientbase_id']) == 4:
                day_data[2] += (now - per_data['datetime']).days
                data_member_count[2] += 1
            elif per_data_count.get(per_data['clientbase_id']) == 5:
                day_data[3] += (now - per_data['datetime']).days
                data_member_count[3] += 1
            else:
                day_data[4] += (now - per_data['datetime']).days
                data_member_count[4] += 1
        print('per_data_count: ', per_data_count)
        f_data_member_count = [1 for count in data_member_count if count == 0]
        data = [ math.ceil(day / count) for day, count in zip(day_data, f_data_member_count)]
        self.notes.update({
            'tooltip_value': f'{{data}} 天 ',
            'tooltip_name': ' '
        })

        self.create_label(data=data, notes=self.notes)

@overview_charts.chart(name='NESL累計圖')
class NESLHorBar(HorizontalBarChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))

    def explain_y(self):
        return '人數'

    def explain_x(self):
        return '會員類型'

    def get_labels(self):
        labels = []
        for days in self.trace_days:
            labels.append(f'{days} 天前')
        # return ['N', 'E', 'S', 'L']
        return labels

    def get_labels_info(self):
        labels = []
        for days in self.trace_days:
            now = timezone.now()
            date_string = (now - datetime.timedelta(days=days)).strftime('%Y 年 %m 月 %d 日')
            labels.append(f'{date_string} ~ {date_string}')
        return labels

    def draw(self):
        now = timezone.now()
        date_start, date_end = self.get_date_range('time_range')
        levels = ['N','E','S','L']
        for level in levels:
            data = []
            for days in self.trace_days:
                date = now - datetime.timedelta(days=days)
                date_range_in_ne_group = [(date - datetime.timedelta(days=90)), date]
                date_range_in_sl_group = [(date - datetime.timedelta(days=365)), date - datetime.timedelta(days=120)]
                purchase_set = PurchaseBase.objects.filter(removed=False)
                # get NE group data in range
                purchasebase_ne_group=purchase_set.filter(datetime__lte=date_range_in_ne_group[1], datetime__gte=date_range_in_ne_group[0])
                # get SL group data in range
                purchasebase_sl_group=purchase_set.filter(datetime__lte=date_range_in_sl_group[1], datetime__gte=date_range_in_sl_group[0])
                # get all NE group clients id
                purchasebase_ne_group_cli_id = purchasebase_ne_group.values_list('clientbase_id',flat=True)
                # get all SL group
                purchasebase_sl_group_cli_id = purchasebase_sl_group.values_list('clientbase_id',flat=True)
                # get real NE data(exclude SL data)
                ne_group_id = purchasebase_ne_group_cli_id.exclude(clientbase_id__in=list(purchasebase_sl_group_cli_id))
                # get real SL data(exclude NE data)
                sl_group_id = purchasebase_sl_group_cli_id.exclude(clientbase_id__in=list(purchasebase_ne_group_cli_id))
                # get NE count data
                ne_group_count = ne_group_id.annotate(Count('clientbase_id')).values('clientbase_id','clientbase_id__count')
                # get SL count data
                sl_group_count = sl_group_id.annotate(Count('clientbase_id')).values('clientbase_id','clientbase_id__count')
                # S count
                s_count = sl_group_count.filter(clientbase_id__count=1).count()
                # L count
                l_count = sl_group_count.exclude(clientbase_id__count=1).count()
                # N count
                n_count = ne_group_count.filter(clientbase_id__count=1).count()
                # E count
                e_count = ne_group_count.exclude(clientbase_id__count=1).count()

                # jugde data
                if level == 'N':
                    data.append(n_count)
                elif level == 'E':
                    data.append(e_count)
                elif level == 'S':
                    data.append(s_count)
                else:
                    data.append(l_count)
            notes = {
                'tooltip_value': '{data} 人',
                'tooltip_name': ' '
            }
            self.create_label(name=level, data=data, notes=notes)


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

    def explain_y(self):
        return '人數'

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
    TURNOVER = 'turnover'
    PERCUSPRICE = 'per_cus_price'
    AVGPRICE = 'avg_price'
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
            select_option=ModeCondition('').choice(
                {'id': self.TURNOVER, 'text': '營業額'},
                {'id': self.PERCUSPRICE, 'text': '客單價'},
                {'id': self.AVGPRICE, 'text': '平均金額'},
            ).default(self.TURNOVER)
        )

    def explain_y(self):
        return '金額'

    def get_total_price_sum(self, query_set, date):
        return query_set.aggregate(Sum('total_price'))

    def filter_date(self, query_set, date):
        return query_set.filter(removed='False').filter(datetime__lt=date)

    def get_turnover_data(self, query_set, date):
        qs_result_dict =  query_set.filter(datetime__lt=date).aggregate(Sum('total_price'))
        return qs_result_dict.get('total_price__sum', 0)

    def get_avg_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        order_count = query_set.filter(datetime__lt=date).count()
        if order_count:
            return math.ceil(turn_over / order_count)
        else:
            return 0

    def get_per_cus_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        member_count = query_set.values('clientbase_id').distinct().filter(datetime__lt=date).count()
        if member_count:
            return math.ceil(turn_over / member_count)
        else:
            return 0

    def get_data_router(self, option, query_set, date):
        if option == self.TURNOVER:
            return self.get_turnover_data(query_set, date)
        elif option == self.AVGPRICE:
            return self.get_avg_price_data(query_set, date)
        elif option == self.PERCUSPRICE:
            return self.get_per_cus_price_data(query_set, date)

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
        select_option = self.options.get('select_option', '未設定')
        self.trace_days = self.options.get('trace_days', self.trace_days)
        data = []
        now = timezone.now()
        purchase_base_set = PurchaseBase.objects.filter(removed=False)
        for days in self.trace_days:
            date = now - datetime.timedelta(days=days)
            result = self.get_data_router(select_option, purchase_base_set, date)
            data.append(result)
        notes = {
            'tooltip_value': '{data} 元'
        }

        self.create_label(name='', data=data, notes=notes)

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

    def explain_y(self):
        return '單數'

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

@past_charts.chart(name='NESL往期直條圖')
class NESLCount(BarChart):
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

    def explain_y(self):
        return '人數'

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
        levels = ['N','E','S','L']
        for level in levels:
            data = []
            for days in self.trace_days:
                date = now - datetime.timedelta(days=days)
                date_range_in_ne_group = [(date - datetime.timedelta(days=90)), date]
                date_range_in_sl_group = [(date - datetime.timedelta(days=365)), date - datetime.timedelta(days=120)]
                purchase_set = PurchaseBase.objects.filter(removed=False)
                # get NE group data in range
                purchasebase_ne_group=purchase_set.filter(datetime__lte=date_range_in_ne_group[1], datetime__gte=date_range_in_ne_group[0])
                # get SL group data in range
                purchasebase_sl_group=purchase_set.filter(datetime__lte=date_range_in_sl_group[1], datetime__gte=date_range_in_sl_group[0])
                # get all NE group clients id
                purchasebase_ne_group_cli_id = purchasebase_ne_group.values_list('clientbase_id',flat=True)
                # get all SL group
                purchasebase_sl_group_cli_id = purchasebase_sl_group.values_list('clientbase_id',flat=True)
                # get real NE data(exclude SL data)
                ne_group_id = purchasebase_ne_group_cli_id.exclude(clientbase_id__in=list(purchasebase_sl_group_cli_id))
                # get real SL data(exclude NE data)
                sl_group_id = purchasebase_sl_group_cli_id.exclude(clientbase_id__in=list(purchasebase_ne_group_cli_id))
                # get NE count data
                ne_group_count = ne_group_id.annotate(Count('clientbase_id')).values('clientbase_id','clientbase_id__count')
                # get SL count data
                sl_group_count = sl_group_id.annotate(Count('clientbase_id')).values('clientbase_id','clientbase_id__count')
                # S count
                s_count = sl_group_count.filter(clientbase_id__count=1).count()
                # L count
                l_count = sl_group_count.exclude(clientbase_id__count=1).count()
                # N count
                n_count = ne_group_count.filter(clientbase_id__count=1).count()
                # E count
                e_count = ne_group_count.exclude(clientbase_id__count=1).count()

                # jugde data
                if level == 'N':
                    data.append(n_count)
                elif level == 'E':
                    data.append(e_count)
                elif level == 'S':
                    data.append(s_count)
                else:
                    data.append(l_count)
            notes = {
                'tooltip_value': '{data} 人',
                'tooltip_name': ' '
            }
            self.create_label(name=level, data=data, notes=notes)


@trend_charts.chart(name='交易金額折線圖(集團)')
class PurchasePriceTrend(LineChart):
    TURNOVER = 'turnover'
    PERCUSPRICE = 'per_cus_price'
    AVGPRICE = 'avg_price'
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )),
            select_option=ModeCondition('').choice(
                {'id': self.TURNOVER, 'text': '營業額'},
                {'id': self.PERCUSPRICE, 'text': '客單價'},
                {'id': self.AVGPRICE, 'text': '平均金額'},
            ).default(self.TURNOVER)
        )


    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('全部品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()

        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])

        self.add_options(all_brand=brand_selection)

    def explain_y(self):
        return '金額'

    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def get_turnover_data(self, query_set, date):
        qs_result_dict =  query_set.filter(datetime__date=date.date()).aggregate(Sum('total_price'))
        result = 0
        if qs_result_dict.get('total_price__sum'):
            result = qs_result_dict.get('total_price__sum')
        return result

    def get_avg_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        order_count = query_set.filter(datetime__date=date.date()).count()
        if order_count:
            return math.ceil(turn_over / order_count)
        else:
            return 0

    def get_per_cus_price_data(self, query_set, date):
        turn_over = self.get_turnover_data(query_set, date)
        member_count = query_set.values('clientbase_id').distinct().filter(datetime__date=date.date()).count()
        if member_count:
            return math.ceil(turn_over / member_count)
        else:
            return 0

    def get_data_router(self, option, query_set, date):
        if option == self.TURNOVER:
            return self.get_turnover_data(query_set, date)
        elif option == self.AVGPRICE:
            return self.get_avg_price_data(query_set, date)
        elif option == self.PERCUSPRICE:
            return self.get_per_cus_price_data(query_set, date)

    def explain_y(self):
        return '金額'


    def draw(self):
        # brand option selection
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        if select_brand_id is None:
            purchase_base_qs = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        elif select_brand_id != 'all':
            purchase_base_qs = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id)
        else:
            purchase_base_qs = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        date_start, date_end = self.get_date_range('time_range')
        date_list = self.get_per_date_list(date_start, date_end)
        self.set_date_range(date_start, date_end)
        # price count option
        select_option = self.options.get('select_option','')
        self.set_total(len(purchase_base_qs))
        data = []
        for date in date_list:
            result = self.get_data_router(select_option, purchase_base_qs, date)
            data.append(result)
        self.notes.update({
                'tooltip_value': '{data} 元',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)
@trend_charts.chart(name='交易人數折線圖(集團)')
class PurchaseMemberTrend(LineChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))
    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()


        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])


        self.add_options(all_brand=brand_selection)


    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '人數'

    def draw(self):
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        if select_brand_id is None:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        elif select_brand_id != 'all':
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id)
        else:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        date_start, date_end = self.get_date_range('time_range')
        date_list = self.get_per_date_list(date_start, date_end)
        self.set_date_range(date_start, date_end)
        self.set_total(len(purchase_base_set))
        data = []
        now = timezone.now()
        for date in date_list:
            order_member = purchase_base_set.filter(removed=False).values('clientbase_id').distinct().filter(datetime__date=date.date())
            data.append(order_member.count())
        self.notes.update({
                'tooltip_value': '{data} 人',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)

@trend_charts.chart(name='交易單數折線圖(集團)')
class PurchaseOrderTrend(LineChart):
    def __init__(self):
        super().__init__()
        now = timezone.now()
        self.add_options(time_range=DateRangeCondition('時間範圍').default(
            (
                (now - datetime.timedelta(days=90)).isoformat(),
                now.isoformat()
            )
        ))
    def init_user(self):
        authedbrand_ids = []
        brand_selection = DropDownCondition('品牌')

        authed_brand_ids = self.get_teamauth_brand_ids()


        brands = self.get_brand_data(authed_brand_ids)

        if len(brands) == Brand.objects.filter(removed='False').count():
            brands.append({'id': 'all', 'text': '全部品牌'})
        if not brands:
             brand_selection.choice(*brands).default('no brand')
        else:
            brand_selection.choice(*brands).default(brands[0]['id'])

        self.add_options(all_brand=brand_selection)


    def get_teamauth_brand_ids(self):
        teamauth = self.user.teamauth_set.filter(team=self.team).first()
        if teamauth is None:
            return []
        enabled_brand_auths = teamauth.brandauth_set.filter(enabled=True)
        authed_brand_ids = enabled_brand_auths.values_list('brand_id', flat=True).values('brand_id')
        return authed_brand_ids


    def get_brand_data(self, authed_brand_ids):
        brand_data = []
        brand_set = Brand.objects.filter(removed='False').filter(id__in=authed_brand_ids).values('id','name')
        for item in brand_set:
            id_name_map = {}
            id_name_map['id'] = item['id']
            id_name_map['text'] = item['name']
            brand_data.append(id_name_map)
        return brand_data

    def get_per_date_list(self, start_date, end_date):
        date_list = []
        delta = end_date - start_date
        for i in range(delta.days + 1):
            date_list.append(start_date + datetime.timedelta(days=i))
        return date_list

    def explain_y(self):
        return '單數'

    def draw(self):
        select_brand_id = self.options.get('all_brand')
        teamauth_brands = self.get_teamauth_brand_ids()
        teamauth_brands = [brand['brand_id'] for brand in teamauth_brands]
        if select_brand_id is None:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        elif select_brand_id != 'all':
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand=select_brand_id)
        else:
            purchase_base_set = PurchaseBase.objects.filter(removed=False).filter(brand__in=teamauth_brands)
        date_start, date_end = self.get_date_range('time_range')
        date_list = self.get_per_date_list(date_start, date_end)
        self.set_date_range(date_start, date_end)
        self.set_total(len(purchase_base_set))
        data = []
        now = timezone.now()
        for date in date_list:
            purchase_base = purchase_base_set.filter(datetime__date=date.date())
            data.append(purchase_base.count())
        self.notes.update({
                'tooltip_value': '{data} 單',
                'tooltip_name': ' '
            })

        self.create_label(data=data, notes=self.notes)




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

GROUP_MEMBER_ORDERS = '會員交易指標'
ORDER_AVG_METRICS = '平均交易指標'
@dashboard_preset
class Purchase:
    name = '交易模組'
    charts = [
        # PurchaseIndicators.preset('會員交易指標', width = 'quarter'),
        TurnOverCard.preset('營業額', group=GROUP_MEMBER_ORDERS),
        PurchaseMemberCard.preset('會員交易人數', group=GROUP_MEMBER_ORDERS),
        PurchaseMemberRateCard.preset('會員交易率', group=GROUP_MEMBER_ORDERS),
        PurchaseCountCard.preset('交易筆數', group=GROUP_MEMBER_ORDERS),
        AvgPriceCard.preset('平均金額', group=ORDER_AVG_METRICS),
        AvgPerMemberCard.preset('客單價', group=ORDER_AVG_METRICS),
        PurchaseTimeHeatMap.preset('交易時間熱區圖', width='full'),
        AvgPerMemberRange.preset('交易客單價區間單數直條圖', width='full'),
        PurchaseNumberBar.preset('交易金額直條圖(集團)', width='full'),
        PurchaseMemCountBar.preset('交易人數直條圖(集團)', width='full'),
        PurchaseOrderBar.preset('交易單數直條圖(集團)', width='full'),
        RFHeatMap.preset('RF分析'),
        RFMHeatMap.preset('RFM分析'),
        RFMCountBar.preset('RFM 分數人數直條圖', width='full'),
        RepurchaseMemCountBar.preset('交易回購人數直條圖'),
        RepurchaseDayCountBar.preset('交易回購天數直條圖'),
        # NESLHorBar.preset('NESL累計圖', width='full'),
        PurchaseMemberCount.preset('交易人數往期直條圖', chart_type='bar'),
        PurchaseNumberCount.preset('交易金額往期直條圖', chart_type='bar'),
        PurchaseOrderCount.preset('交易單數往期直條圖', chart_type='bar'),
        NESLCount.preset('NESL往期直條圖', chart_type='bar'),
        PurchasePriceTrend.preset('交易金額折線圖(集團)', width='full'),
        PurchaseMemberTrend.preset('交易人數折線圖(集團)', width='full'),
        PurchaseOrderTrend.preset('交易單數折線圖(集團)', width='full')
    ]