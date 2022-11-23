from typing import Any, Tuple

from dateutil.relativedelta import relativedelta

from django.utils import timezone
from django.db.models.query import Q
from django.db.models.functions import Coalesce, Cast
from django.db.models import QuerySet, Count, Avg, F, IntegerField, OuterRef, Subquery, Sum, CharField, TextField, Min, Max
from django.contrib.postgres.aggregates import ArrayAgg
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields.jsonb import KeyTextTransform

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition, SingleSelectCondition, MultiCheckBoxCondition
from filtration.registries import condition
from filtration.exceptions import UseIdList

from tag_assigner.models import ValueTag

from cerem.tasks import aggregate_from_cerem
from .models import PurchaseBase

@condition('品牌名稱', tab='訂單記錄')
class Brands(SelectCondition):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        client_qs = client_qs.annotate(
            order_brand_ids=Subquery(
                PurchaseBase.objects.filter(clientbase_id=OuterRef('id'), removed=False, status=PurchaseBase.STATUS_CONFIRMED)
                .annotate(brand_ids=ArrayAgg('brand_id'))
                .values('brand_ids')[:1], output_field=ArrayField(IntegerField())
            ),
        )
        if self.options.get('intersection', False):
            q = Q(order_brand_ids__contains=choices)
        else:
            q = Q(order_brand_ids__overlap=choices)

        return client_qs, q

    def lazy_init(self, team, *args, **kwargs):
        brands = team.brand_set.filter(removed=False).order_by('order').values('id', text=F('name'))

        data = list(brands)

        self.choice(*data)


class PurchaseValuesConditionBase(RangeCondition):
    ATTRIBUTION_KEY = '門市名稱'
    minimum = 0
    maximum = 10000

    def real_time_init(self, team, *args, **kwargs):
        brand_choices = team.brand_set.filter(removed=False).values('id', text=F('name')).order_by('order')
        shops = team.purchasebase_set.filter(removed=False).values_list(f'attributions__{self.ATTRIBUTION_KEY}', flat=True).distinct()
        shop_choices = [
            {'id': shop_name, 'text': shop_name} for shop_name in shops
        ]
        self.range(self.minimum, self.maximum)
        self.add_options(
            date_range=DateRangeCondition('日期區間'),
            brand_ids=SelectCondition('品牌').choice(*brand_choices),
            shops=SelectCondition('門市').choice(*shop_choices)
        )

    def get_order_filter(self):
        params = {}
        shops = self.options.get('shops')
        brand_ids = self.options.get('brand_ids')
        date_range = self.options.get('date_range')
        if shops:
            params[f'purchasebase__attributions__{self.ATTRIBUTION_KEY}__in'] = self.options.get('shops', [])
        if brand_ids:
            params['purchasebase__brand_id__in'] = brand_ids
        if date_range:
            params['purchasebase__datetime__range'] = date_range
        order_filter = Q(
            purchasebase__removed=False, purchasebase__status=PurchaseBase.STATUS_CONFIRMED, **params
        )
        return order_filter


@condition('營業額', tab='訂單記錄')
class TotalSales(PurchaseValuesConditionBase):

    def filter(self, client_qs: QuerySet, value_range: Any) -> Tuple[QuerySet, Q]:

        client_qs = client_qs.annotate(total_sales=Coalesce(Sum('purchasebase__total_price', filter=self.get_order_filter()), 0))
        q = Q(total_sales__range=value_range)

        return client_qs, q


@condition('平均金額', tab='訂單記錄')
class AvgAmount(PurchaseValuesConditionBase):
    minimum = 0
    maximum = 1000

    def filter(self, client_qs: QuerySet, value_range: Any) -> Tuple[QuerySet, Q]:

        client_qs = client_qs.annotate(avg_amount=Coalesce(Avg('purchasebase__total_price', filter=self.get_order_filter()), 0))
        q = Q(avg_amount__range=value_range)

        return client_qs, q


@condition('交易單數', tab='訂單記錄')
class OrderCount(PurchaseValuesConditionBase):
    minimum = 0
    maximum = 50

    def filter(self, client_qs: QuerySet, value_range: Any) -> Tuple[QuerySet, Q]:

        client_qs = client_qs.annotate(order_count=Coalesce(Count('purchasebase__total_price', filter=self.get_order_filter()), 0))
        q = Q(order_count__range=value_range)

        return client_qs, q


@condition('門市名稱', tab='訂單記錄')
class Shops(SelectCondition):
    ATTRIBUTION_KEY = '門市名稱'
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        client_qs = client_qs.annotate(
            order_shop_names=Coalesce(
                Subquery(
                    PurchaseBase.objects.filter(
                        clientbase_id=OuterRef('id'), removed=False, status=PurchaseBase.STATUS_CONFIRMED,
                        attributions__has_key=self.ATTRIBUTION_KEY
                    )
                    .annotate(shop_name=KeyTextTransform(self.ATTRIBUTION_KEY, 'attributions'))
                    .annotate(shop_names=ArrayAgg('shop_name'))
                    .values('shop_names')[:1], output_field=ArrayField(TextField())
                ), []
            )
        )
        print(client_qs.values('order_shop_names'), choices)
        if self.options.get('intersection', False):
            q = Q(order_shop_names__contains=choices)
        else:
            q = Q(order_shop_names__overlap=choices)

        return client_qs, q

    def lazy_init(self, team, *args, **kwargs):
        shops = team.purchasebase_set.filter(removed=False).values_list(f'attributions__{self.ATTRIBUTION_KEY}', flat=True).distinct()

        data = [
            {'id': shop_name, 'text': shop_name} for shop_name in shops if shop_name
        ]

        self.choice(*data)


@condition('單筆金額', tab='訂單記錄')
class AnyOrderPriceRange(RangeCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.range(0, 5000).config(postfix='元')

    def filter(self, client_qs: QuerySet, value_range: Any) -> Tuple[QuerySet, Q]:

        q = Q(purchasebase__total_price__range=value_range)

        return client_qs, q

@condition('RFM', tab='訂單記錄')
class RFM(Condition):
    class LEVEL:
        HIGH = 'high'
        MID = 'mid'
        LOW = 'low'
        CHOICES = {
            HIGH: '高',
            MID: '中',
            LOW: '低'
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        choices = []
        for choice_key, name in self.LEVEL.CHOICES.items():
            choices.append({'id': choice_key, 'text': name})
        self.add_options(
            r_level=MultiCheckBoxCondition('R').choice(*choices),
            f_level=MultiCheckBoxCondition('F').choice(*choices),
            m_level=MultiCheckBoxCondition('M').choice(*choices)
        )

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        r_levels = self.options.get('r_level', [])
        f_levels = self.options.get('f_level', [])
        m_levels = self.options.get('m_level', [])

        r_scores = []
        f_scores = []
        m_scores = []

        if self.LEVEL.HIGH in r_levels:
            r_scores += [4, 5]
        if self.LEVEL.MID in r_levels:
            r_scores += [3]
        if self.LEVEL.LOW in r_levels:
            r_scores += [1, 2]

        if self.LEVEL.HIGH in f_levels:
            f_scores += [4, 5]
        if self.LEVEL.MID in f_levels:
            f_scores += [3]
        if self.LEVEL.LOW in f_levels:
            f_scores += [1, 2]

        if self.LEVEL.HIGH in m_levels:
            m_scores += [4, 5]
        if self.LEVEL.MID in m_levels:
            m_scores += [3]
        if self.LEVEL.LOW in m_levels:
            m_scores += [1, 2]

        q = Q(rfm_recency__in=r_scores, rfm_frequency__in=f_scores, rfm_monetary__in=m_scores)
        print(r_scores, f_scores, m_scores)
        return client_qs, q


@condition('NESL', tab='訂單記錄')
class NESL(MultiCheckBoxCondition):
    class TYPE:
        NO_ORDER = 'no_order'
        LOST = 'lost'
        SLEEPING = 'sleeping'
        ACTIVE = 'active'
        NEW = 'new'
        CHOICES = {
            NO_ORDER: '未購會員',
            LOST: '流失會員',
            SLEEPING: '瞌睡會員',
            ACTIVE: '主力會員',
            NEW: '新會員'
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        choices = []
        for choice_key, name in self.TYPE.CHOICES.items():
            choices.append({'id': choice_key, 'text': name})
        self.choice(*choices)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        recent_datetime = now - relativedelta(months=3)
        period_datetime = now - relativedelta(months=6)
        order_filter = Q(purchasebase__removed=False, purchasebase__status=PurchaseBase.STATUS_CONFIRMED)
        client_qs = client_qs.annotate(
            first_purchase=Min('purchasebase__datetime', filter=order_filter), purchase_count=Count('purchasebase', filter=order_filter),
            last_purchase=Max('purchasebase__datetime', filter=order_filter)
        )
        q = Q()
        if self.TYPE.NO_ORDER in choices:
            q |= Q(purchase_count=0)
        if self.TYPE.NEW in choices:
            q |= Q(first_purchase__gte=recent_datetime, purchase_count=1)
        if self.TYPE.SLEEPING in choices:
            q |= Q(last_purchase__lt=recent_datetime, purchase_count__gt=1)
        if self.TYPE.ACTIVE in choices:
            q |= Q(rfm_percentile__gt=8)
        if self.TYPE.LOST in choices:
            q |= Q(last_purchase__lt=period_datetime, purchase_count__gt=1)

        return client_qs, q
