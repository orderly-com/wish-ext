import datetime
import itertools
import statistics
from apyori import apriori
from dateutil import relativedelta
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, fpmax, fpgrowth, association_rules

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


@product_charts.chart(name='商品併買提升度圖')
class TopProductSetLifts(BarChart):

    def explain_x(self):
        return '商品組合'

    def explain_y(self):
        return '提升度 - lift'

    def draw(self):
        min_support = self.options.get('min_support', 0.01)
        min_confidence = self.options.get('min_confidence', 0.01)
        display_count = self.options.get('display_count', 50)
        qs = (
            self.team.purchasebase_set.select_related('clientbase')
            .filter(
                orderproduct__productbase_id__isnull=False,
                status=PurchaseBase.STATUS_CONFIRMED,
                removed=False,
                datetime__gte=settings.FIRST_DATE,
                clientbase__internal_member=False,
                clientbase__removed=False)
            .order_by('datetime')
            .annotate(products=ArrayAgg(Cast('orderproduct__productbase_id', CharField())))
            .values_list('products', flat=True)
        )
        dataset = list(qs)
        te = TransactionEncoder()
        te_ary = te.fit(dataset).transform(dataset)
        df = pd.DataFrame(te_ary, columns=te.columns_)
        labels = []
        data_array = []
        frequent_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True, max_len=2)
        df = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        df.sort_values(by='lift', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        for index, row in df.iterrows():
            if index >= display_count:
                break
            product_names = []
            for item in row['consequents']:
                productbase = self.team.productbase_set.filter(id=item).first()
                product_names.append(productbase.name)
            for item in row['antecedents']:
                productbase = self.team.productbase_set.filter(id=item).first()
                product_names.append(productbase.name)
            labels.append(', '.join(product_names))
            data_array.append(row['lift'])
        self.set_labels(labels)
        self.create_label(name='lift', data=data_array)


@product_charts.chart(name='商品併買支持度圖')
class TopProductSetSupports(BarChart):

    def explain_x(self):
        return '商品組合'

    def explain_y(self):
        return '支持度 - support'


    def draw(self):
        min_support = self.options.get('min_support', 0.01)
        display_count = self.options.get('display_count', 50)
        qs = (
            self.team.purchasebase_set.select_related('clientbase')
            .filter(
                orderproduct__productbase_id__isnull=False,
                status=PurchaseBase.STATUS_CONFIRMED,
                removed=False,
                datetime__gte=settings.FIRST_DATE,
                clientbase__internal_member=False,
                clientbase__removed=False)
            .order_by('datetime')
            .annotate(products=ArrayAgg(Cast('orderproduct__productbase_id', CharField())))
            .values_list('products', flat=True)
        )
        dataset = list(qs)
        te = TransactionEncoder()
        te_ary = te.fit(dataset).transform(dataset)
        df = pd.DataFrame(te_ary, columns=te.columns_)
        labels = []
        data_array = []
        frequent_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True, max_len=2)
        frequent_itemsets.sort_values(by='support', ascending=False, inplace=True)
        frequent_itemsets.reset_index(drop=True, inplace=True)
        for index, row in frequent_itemsets.iterrows():
            if index >= display_count:
                break
            product_names = []
            for item in row['itemsets']:
                productbase = self.team.productbase_set.filter(id=item).first()
                product_names.append(productbase.name)
            labels.append(', '.join(product_names))
            data_array.append(row['support'])
        self.set_labels(labels)
        self.create_label(name='support', data=data_array)


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
