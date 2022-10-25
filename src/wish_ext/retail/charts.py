import datetime
import itertools
import statistics
from apyori import apriori
from dateutil import relativedelta

from django.utils import timezone
from django.conf import settings
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast
from django.db.models import Count, Func, Max, Min, IntegerField
from django.contrib.postgres.aggregates import ArrayAgg

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart, HorizontalBarChart, DataCard
from filtration.conditions import DateRangeCondition, ModeCondition

from orderly_core.team.charts import client_charts
from cerem.tasks import clickhouse_client
from cerem.utils import F

from .models import PurchaseBase

rfm_charts = chart_category('rfm', 'RFM')

product_charts = chart_category('product', '商品')


@rfm_charts.chart(name='RFM 人數直條圖')
class RFMClientCount(BarChart):

    def explain_x(self):
        return 'RFM 分數'

    def explain_y(self):
        return '人數'

    def get_labels(self):
        return list(range(1, 16))

    def draw(self):
        clients = self.team.clientbase_set.filter(removed=False)
        clients = clients.values('rfm_total_score').annotate(count=Count('id')).values_list('rfm_total_score', 'count')
        data = {}
        for score, count in clients:
            data[score] = count
        data_array = []
        for i in range(1, 16):
            data_array.append(data.get(i, 0))
        self.create_label(name='人數', data=data_array)


@product_charts.chart(name='商品併買圖')
class TopProductSets(BarChart):

    def explain_x(self):
        return '商品組合'

    def explain_y(self):
        return '訂單數'


    def draw(self):
        qs = self.team.purchasebase_set.select_related('clientbase') \
            .filter(
                orderproduct__productbase_id__isnull=False,
                status=PurchaseBase.STATUS_CONFIRMED,
                removed=False,
                datetime__gte=settings.FIRST_DATE,
                clientbase__internal_member=False,
                clientbase__removed=False)\
            .order_by('datetime')\
            .annotate(products=ArrayAgg('orderproduct__productbase_id'))
        data = list(qs.values_list('products', flat=True))
        association_rules = apriori(data, max_length=2, min_support=0.01, min_confidence=0.2, min_lift=3)
        association_results = [itemset for itemset in association_rules if len(itemset.items) > 1]
        labels = []
        data_array = []
        for itemset in sorted(association_results, key=lambda x:x.support, reverse=True)[:10]:
            product_names = []
            for item in itemset.items:
                productbase = self.team.productbase_set.filter(id=item).first()
                product_names.append(productbase.name)
            labels.append(', '.join(product_names))
            data_array.append(round(itemset.support * len(data)))
        self.set_labels(labels)
        self.create_label(name='組合出現頻率', data=data_array)
        print(data)
        print(association_results)


@client_charts.chart(name='NESL 人數直條圖')
class NESLClientCount(BarChart):

    def explain_x(self):
        return '分群'

    def explain_y(self):
        return '人數'

    def get_labels(self):
        return ['N', 'E', 'S', 'L']

    def draw(self):
        now = timezone.now()
        clients = self.team.clientbase_set.filter(removed=False)
        recent_datetime = now - relativedelta.relativedelta(months=3)
        clients = clients.annotate(first_purchase=Min('purchasebase__datetime'), purchase_count=Count('purchasebase'))

        new = clients.filter(first_purchase__gte=recent_datetime).filter(purchase_count=1).count()
        existing = clients.filter(first_purchase__gte=recent_datetime).filter(purchase_count__gt=1).count()
        sleeping = clients.exclude(first_purchase__lt=recent_datetime).filter(purchase_count__gt=1).count()
        others = max(clients.count() - new - existing - sleeping, 0)
        self.create_label(name='人數', data=[new, existing, sleeping, others])
