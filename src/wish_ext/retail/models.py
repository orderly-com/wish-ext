from django.urls import reverse
import html2text
from uuid import uuid4

from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

from datahub.models import DataSource

from core.models import BaseModel, ValueTaggable
from team.models import Team, OrderBase, ProductBase, ClientBase
from team.registries import client_info_model
from tag_assigner.registries import taggable

from cerem.utils import TeamMongoDB, F, Sum

from ..extension import wish_ext
from ..wish.models import Brand


class RepurchaseCycle(BaseModel):

    class Meta:

        indexes = [
            models.Index(fields=['team', ]),
        ]

    team = models.ForeignKey(Team, blank=True, null=True, on_delete=models.CASCADE)
    purchase_append_days = models.IntegerField(default=2)  # if an order day is less than this number, it's a append purcahse (not a re-purchase)

    # count
    count_cycle = models.IntegerField(default=0)            # 計算總共出現多少次回購
    count_of_clientbase = models.IntegerField(default=0)    # 多少回購人

    # clientbase
    clientbases = ArrayField(models.IntegerField(), default=list)  # [ all clients ] -> we can us this field to map to all XXXX_of_each_client

    # daybase
    daybase = ArrayField(models.IntegerField(), default=list)  # [ all days_between ]
    # cycle_daybase_of_each_client = ArrayField(ArrayField(models.IntegerField(), default=list), default=list)  # [[each clientbase], [each clientbase], [each clientbase]...]
    cycle_daybase_of_each_client = JSONField(blank=True, null=True)  # [[each clientbase], [each clientbase], [each clientbase]...]
    # cycle_daybase = ArrayField(ArrayField(models.IntegerField(), default=list), default=list)  # [[ all first repurchase days_between ], [ all second repurchase days_between],[...], ...]
    cycle_daybase = JSONField(blank=True, null=True)  # [[ all first repurchase days_between ], [ all second repurchase days_between],[...], ...]
    count_of_cycle_daybase = ArrayField(models.IntegerField(), default=list)  # [ len([ all first repurchase days_between ]), len([ all second repurchase days_between]), ...]

    # mean daybase / median daybase
    mean_of_daybase = models.FloatField(default=-1)             # daybase - mean
    median_of_daybase = models.IntegerField(default=-1)         # daybase - median
    median_low_of_daybase = models.IntegerField(default=-1)     # daybase - median low
    median_high_of_daybase = models.IntegerField(default=-1)    # daybase - median high
    mode_of_daybase = models.IntegerField(default=-1)           # daybase - mode
    pstdev_of_daybase = models.FloatField(default=-1)           # daybase - pstdev
    pvariance_of_daybase = models.FloatField(default=-1)        # daybase - pvariance
    stdev_of_daybase = models.FloatField(default=-1)            # daybase - stdev
    variance_of_daybase = models.FloatField(default=-1)         # daybase - variance

    essentialized_daybase = ArrayField(models.IntegerField(), default=list)
    low_of_essentialized_daybase = models.IntegerField(default=-1)
    high_of_essentialized_daybase = models.IntegerField(default=-1)
    stdev_of_essentialized_daybase = models.FloatField(default=-1)
    forced_of_essentialized_daybase = models.FloatField(default=-1)  # set this value if team wants to set it manually

    mean_of_each_cycle_daybase = ArrayField(models.FloatField(), default=list)       # cycle_daybase - mean
    median_of_each_cycle_daybase = ArrayField(models.IntegerField(), default=list)   # cycle_daybase - median
    mode_of_each_cycle_daybase = ArrayField(models.IntegerField(), default=list)     # cycle_daybase - mode
    pstdev_of_each_cycle_daybase = ArrayField(models.FloatField(), default=list)     # cycle_daybase - pstdev
    pvariance_of_each_cycle_daybase = ArrayField(models.FloatField(), default=list)  # cycle_daybase - pvariance
    stdev_of_each_cycle_daybase = ArrayField(models.FloatField(), default=list)      # cycle_daybase - stdev
    variance_of_each_cycle_daybase = ArrayField(models.FloatField(), default=list)   # cycle_daybase - variance

    # cycle_weekbase, count_of_cycle_weekbase
    weekbase = ArrayField(models.IntegerField(), default=list)  # [ all days_between ]
    # cycle_weekbase_of_each_client = ArrayField(ArrayField(models.IntegerField(), default=list), default=list)  # [[each clientbase], [each clientbase], [each clientbase]...]
    cycle_weekbase_of_each_client = JSONField(blank=True, null=True)  # [[each clientbase], [each clientbase], [each clientbase]...]
    # cycle_weekbase = ArrayField(ArrayField(models.IntegerField(), default=list), default=list)  # [[ all first repurchase cycle_weekbase ], [ all second repurchase cycle_weekbase],[...], ...]
    cycle_weekbase = JSONField(blank=True, null=True)  # [[ all first repurchase cycle_weekbase ], [ all second repurchase cycle_weekbase],[...], ...]
    count_of_cycle_weekbase = ArrayField(models.IntegerField(), default=list)  # [ len([ all first repurchase cycle_weekbase ]), len([ all second repurchase cycle_weekbase]), ...]

    mean_of_weekbase = models.FloatField(default=-1)            # weekbase - mean
    median_of_weekbase = models.IntegerField(default=-1)        # weekbase - median
    median_low_of_weekbase = models.IntegerField(default=-1)    # weekbase - median low
    median_high_of_weekbase = models.IntegerField(default=-1)   # weekbase - median high
    mode_of_weekbase = models.IntegerField(default=-1)          # weekbase - mode
    pstdev_of_weekbase = models.FloatField(default=-1)          # weekbase - pstdev
    pvariance_of_weekbase = models.FloatField(default=-1)       # weekbase - pvariance
    stdev_of_weekbase = models.FloatField(default=-1)           # weekbase - stdev
    variance_of_weekbase = models.FloatField(default=-1)        # weekbase - variance

    essentialized_weekbase = ArrayField(models.IntegerField(), default=list)
    low_of_essentialized_weekbase = models.IntegerField(default=-1)
    high_of_essentialized_weekbase = models.IntegerField(default=-1)
    stdev_of_essentialized_weekbase = models.FloatField(default=-1)

    mean_of_each_cycle_weekbase = ArrayField(models.FloatField(), default=list)       # cycle_weekbase - mean
    median_of_each_cycle_weekbase = ArrayField(models.IntegerField(), default=list)   # cycle_weekbase - median
    mode_of_each_cycle_weekbase = ArrayField(models.IntegerField(), default=list)     # cycle_weekbase - mode
    pstdev_of_each_cycle_weekbase = ArrayField(models.FloatField(), default=list)     # cycle_weekbase - pstdev
    pvariance_of_each_cycle_weekbase = ArrayField(models.FloatField(), default=list)  # cycle_weekbase - pvariance
    stdev_of_each_cycle_weekbase = ArrayField(models.FloatField(), default=list)      # cycle_weekbase - stdev
    variance_of_each_cycle_weekbase = ArrayField(models.FloatField(), default=list)   # cycle_weekbase - variance

    def calculate(self, date_end=None, save_data=True):

        def essentialize(data: list) -> tuple:
            length = len(data)
            data = data[int(length * 0.45): length - int(length * 0.45)]

            try:
                stdev = statistics.stdev(data)
            except Exception:
                stdev = 0

            return (data[0], data[len(data) - 1], stdev)

        ft = ForestTimer()

        qs = self.team.orderbase_set.filter(
            status_key=OrderBase.STATUS_CONFIRMED,
            removed=False,
        )

        if date_end is not None:

            qs = qs.filter(datetime__lte=date_end)

        data = qs.order_by('datetime').values_list('clientbase_id', 'datetime')

        ft.step('data')

        # no data to be calculated
        if len(data) == 0:
            return False

        count_cycle = 0
        count_of_clientbase = 0

        daybase = list()
        weekbase = list()
        clientbases = list()

        cycle_daybase_of_each_client = list()
        cycle_weekbase_of_each_client = list()

        purchase_append_days = self.purchase_append_days

        cycle_daybase = dict()
        cycle_weekbase = dict()
        last_date_of_client = dict()
        cycle_count_of_client = dict()
        order_count_of_client = dict()

        for order in data:
            id = order[0]
            dt = order[1]
            # pids = order[2]
            # cost = order[3]

            clientbases.append(id)

            if(id in last_date_of_client):
                if (dt - last_date_of_client[id]).days > purchase_append_days:
                    delta = dt - last_date_of_client[id]
                    cycle_count_of_client[id] += 1
                    days = delta.days
                    weeks = int(days / 7)
                    daybase.append(days)
                    weekbase.append(weeks)
                    count_cycle += 1
                    if cycle_count_of_client[id] not in cycle_daybase:
                        cycle_daybase[cycle_count_of_client[id]] = []
                        cycle_weekbase[cycle_count_of_client[id]] = []

                    if cycle_count_of_client[id] == 1:
                        count_of_clientbase += 1
                    cycle_daybase[cycle_count_of_client[id]].append(days)
                    cycle_weekbase[cycle_count_of_client[id]].append(weeks)
                order_count_of_client[id] += 1
            else:  # first order of this client
                cycle_count_of_client[id] = 0
                order_count_of_client[id] = 0

            last_date_of_client[id] = dt

        ft.step('daybase')

        daybase.sort()
        daybase = [x for x in daybase if x < 365]  # remove element greater than 365

        count_of_cycle_daybase = list()
        count_of_cycle_weekbase = list()
        # count_of_cycle_daybase, count_of_cycle_weekbase, client_of_cycle_weekbase
        for client in cycle_count_of_client.items():
            id = client[0]
            count = client[1]
            while len(count_of_cycle_daybase) <= count:
                count_of_cycle_daybase.append(0)
                count_of_cycle_weekbase.append(0)
            if count not in cycle_daybase:
                cycle_weekbase[count] = list()
                cycle_daybase[count] = list()
            count_of_cycle_daybase[count] += 1
            count_of_cycle_weekbase[count] += 1
            cycle_weekbase[count].append(id)
            cycle_daybase[count].append(id)

        ft.step('cycle_count_of_client')

        # statistics of daybase
        # init
        mean_of_daybase = -1
        median_of_daybase = -1
        median_low_of_daybase = -1
        median_high_of_daybase = -1
        mode_of_daybase = -1
        pstdev_of_daybase = -1
        pvariance_of_daybase = -1
        stdev_of_daybase = -1
        variance_of_daybase = -1

        essentialized_daybase = []
        low_of_essentialized_daybase = -1
        high_of_essentialized_daybase = -1
        stdev_of_essentialized_daybase = -1

        if len(daybase) > 1:

            mean_of_daybase = statistics.mean(daybase)
            median_of_daybase = statistics.median(daybase)
            median_low_of_daybase = statistics.median_low(daybase)
            median_high_of_daybase = statistics.median_high(daybase)

            if len(daybase) > 2:
                try:
                    mode_of_daybase = statistics.mode(daybase)
                except Exception:
                    pass
                pstdev_of_daybase = statistics.pstdev(daybase)
                pvariance_of_daybase = statistics.pvariance(daybase)
                stdev_of_daybase = statistics.stdev(daybase)
                variance_of_daybase = statistics.variance(daybase)

                # statistics of middle 10% daybase
                essentialized_daybase = essentialize(daybase)
                low_of_essentialized_daybase = essentialized_daybase[0]
                high_of_essentialized_daybase = essentialized_daybase[1]
                stdev_of_essentialized_daybase = essentialized_daybase[2]

        ft.step('statistics')

        # statistics of cycle_daybase
        mean_of_each_cycle_daybase = list()
        median_of_each_cycle_daybase = list()
        mode_of_each_cycle_daybase = list()
        pstdev_of_each_cycle_daybase = list()
        pvariance_of_each_cycle_daybase = list()
        stdev_of_each_cycle_daybase = list()
        variance_of_each_cycle_daybase = list()

        for x in cycle_daybase.values():
            mean_of_each_cycle_daybase.append(statistics.mean(x))
            median_of_each_cycle_daybase.append(statistics.median(x))
            try:
                mode_of_each_cycle_daybase.append(statistics.mode(x))  # there might be no unique mode
            except Exception as e:
                del e
                mode_of_each_cycle_daybase.append(-1)

            if len(x) > 2:
                pstdev_of_each_cycle_daybase.append(statistics.pstdev(x))
                pvariance_of_each_cycle_daybase.append(statistics.pvariance(x))
                stdev_of_each_cycle_daybase.append(statistics.stdev(x))
                variance_of_each_cycle_daybase.append(statistics.variance(x))
            else:
                pstdev_of_each_cycle_daybase.append(-1)
                pvariance_of_each_cycle_daybase.append(-1)
                stdev_of_each_cycle_daybase.append(-1)
                variance_of_each_cycle_daybase.append(-1)

        ft.step('cycle_daybase')

        # statistics of weekbase
        # init
        mean_of_weekbase = -1
        median_of_weekbase = -1
        median_low_of_weekbase = -1
        median_high_of_weekbase = -1
        mode_of_weekbase = -1
        pstdev_of_weekbase = -1
        pvariance_of_weekbase = -1
        stdev_of_weekbase = -1
        variance_of_weekbase = -1

        essentialized_weekbase = []
        low_of_essentialized_weekbase = -1
        high_of_essentialized_weekbase = -1
        stdev_of_essentialized_weekbase = -1

        if len(weekbase) > 1:

            mean_of_weekbase = statistics.mean(weekbase)
            median_of_weekbase = statistics.median(weekbase)
            median_low_of_weekbase = statistics.median_low(weekbase)
            median_high_of_weekbase = statistics.median_high(weekbase)

            if len(weekbase) > 2:
                try:
                    mode_of_weekbase = statistics.mode(weekbase)
                except Exception:
                    pass
                pstdev_of_weekbase = statistics.pstdev(weekbase)
                pvariance_of_weekbase = statistics.pvariance(weekbase)
                stdev_of_weekbase = statistics.stdev(weekbase)
                variance_of_weekbase = statistics.variance(weekbase)

                # statistics of middle 10% weekbase
                essentialized_weekbase = essentialize(weekbase)
                low_of_essentialized_weekbase = essentialized_weekbase[0]
                high_of_essentialized_weekbase = essentialized_weekbase[1]
                stdev_of_essentialized_weekbase = essentialized_weekbase[2]

        ft.step('weekbase')

        # statistics of cycle_weekbase
        mean_of_each_cycle_weekbase = list()
        median_of_each_cycle_weekbase = list()
        mode_of_each_cycle_weekbase = list()
        pstdev_of_each_cycle_weekbase = list()
        pvariance_of_each_cycle_weekbase = list()
        stdev_of_each_cycle_weekbase = list()
        variance_of_each_cycle_weekbase = list()

        for x in cycle_weekbase.values():
            mean_of_each_cycle_weekbase.append(statistics.mean(x))
            median_of_each_cycle_weekbase.append(statistics.median(x))
            try:
                mode_of_each_cycle_weekbase.append(statistics.mode(x))  # there might be no unique mode
            except Exception as e:
                del e
                mode_of_each_cycle_weekbase.append(-1)

            if len(x) > 2:
                pstdev_of_each_cycle_weekbase.append(statistics.pstdev(x))
                pvariance_of_each_cycle_weekbase.append(statistics.pvariance(x))
                stdev_of_each_cycle_weekbase.append(statistics.stdev(x))
                variance_of_each_cycle_weekbase.append(statistics.variance(x))
            else:
                pstdev_of_each_cycle_weekbase.append(-1)
                pvariance_of_each_cycle_weekbase.append(-1)
                stdev_of_each_cycle_weekbase.append(-1)
                variance_of_each_cycle_weekbase.append(-1)

        ft.step('cycle_weekbase')

        self.clientbases = list(set(clientbases))

        self.count_cycle = count_cycle
        self.count_of_clientbase = count_of_clientbase
        self.daybase = daybase
        self.cycle_daybase_of_each_client = cycle_daybase_of_each_client
        self.cycle_daybase = cycle_daybase

        # cycle_daybase
        self.count_of_cycle_daybase = count_of_cycle_daybase
        self.mean_of_daybase = mean_of_daybase
        self.median_of_daybase = median_of_daybase
        self.median_low_of_daybase = median_low_of_daybase
        self.median_high_of_daybase = median_high_of_daybase
        self.mode_of_daybase = mode_of_daybase
        self.pstdev_of_daybase = pstdev_of_daybase
        self.pvariance_of_daybase = pvariance_of_daybase
        self.stdev_of_daybase = stdev_of_daybase
        self.variance_of_daybase = variance_of_daybase

        # essentialized_daybase
        self.essentialized_daybase = essentialized_daybase
        self.low_of_essentialized_daybase = low_of_essentialized_daybase
        self.high_of_essentialized_daybase = high_of_essentialized_daybase
        self.stdev_of_essentialized_daybase = stdev_of_essentialized_daybase

        # each_cycle_daybase
        self.mean_of_each_cycle_daybase = mean_of_each_cycle_daybase                # mean
        self.median_of_each_cycle_daybase = median_of_each_cycle_daybase            # median
        self.mode_of_each_cycle_daybase = mode_of_each_cycle_daybase                # mode
        self.pstdev_of_each_cycle_daybase = pstdev_of_each_cycle_daybase            # pstdev
        self.pvariance_of_each_cycle_daybase = pvariance_of_each_cycle_daybase      # pvariance
        self.stdev_of_each_cycle_daybase = stdev_of_each_cycle_daybase              # stdev
        self.variance_of_each_cycle_daybase = variance_of_each_cycle_daybase        # variance

        # weekbase
        self.weekbase = weekbase
        self.count_of_cycle_weekbase = count_of_cycle_weekbase
        self.cycle_weekbase_of_each_client = cycle_weekbase_of_each_client
        self.cycle_weekbase = cycle_weekbase

        # essentialized_weekbase
        self.essentialized_weekbase = essentialized_weekbase
        self.low_of_essentialized_weekbase = low_of_essentialized_weekbase
        self.high_of_essentialized_weekbase = high_of_essentialized_weekbase
        self.stdev_of_essentialized_weekbase = stdev_of_essentialized_weekbase

        self.mean_of_weekbase = mean_of_weekbase
        self.median_of_weekbase = median_of_weekbase
        self.median_low_of_weekbase = median_low_of_weekbase
        self.median_high_of_weekbase = median_high_of_weekbase
        self.mode_of_weekbase = mode_of_weekbase
        self.pstdev_of_weekbase = pstdev_of_weekbase
        self.pvariance_of_weekbase = pvariance_of_weekbase
        self.stdev_of_weekbase = stdev_of_weekbase
        self.variance_of_weekbase = variance_of_weekbase

        # each_cycle_daybase
        self.mean_of_each_cycle_weekbase = mean_of_each_cycle_weekbase              # mean
        self.median_of_each_cycle_weekbase = median_of_each_cycle_weekbase          # median
        self.mode_of_each_cycle_weekbase = mode_of_each_cycle_weekbase              # mode
        self.pstdev_of_each_cycle_weekbase = pstdev_of_each_cycle_weekbase          # pstdev
        self.pvariance_of_each_cycle_weekbase = pvariance_of_each_cycle_weekbase    # pvariance
        self.stdev_of_each_cycle_weekbase = stdev_of_each_cycle_weekbase            # stdev
        self.variance_of_each_cycle_weekbase = variance_of_each_cycle_weekbase      # variance

        if save_data:
            self.save()

            ft.step('save')

        return True


# class ClientRecency(BaseModel):

#     class Meta:

#         indexes = [
#             models.Index(fields=['team', ]),
#             models.Index(fields=['clientbase', ]),
#         ]

#     team = models.ForeignKey(Team, blank=True, null=True, on_delete=models.CASCADE)
#     clientbase = models.ForeignKey(ClientBase, blank=True, null=True, on_delete=models.CASCADE)
#     value = models.IntegerField(default=-1)

class ProductCategory(BaseModel):
    class Meta:

        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['name', ]),

            models.Index(fields=['team', 'name']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)

    external_id = models.TextField(blank=False)
    uuid = models.UUIDField(default=uuid4, unique=True)

    name = models.TextField(blank=False)
    removed = models.BooleanField(default=False)


@wish_ext.ProductModel
@taggable('product')
class RetailProduct(ProductBase):
    class Meta:
        indexes = [
            models.Index(fields=['datasource', ]),

            models.Index(fields=['team', 'datasource']),
        ]

    price = models.FloatField(default=0)

    datasource = models.ForeignKey(DataSource, blank=False, default=1, on_delete=models.CASCADE)

    attributions = JSONField(default=dict)
    categories = models.ManyToManyField(ProductCategory, blank=True)

    def get_detail_url(self):
        return reverse('media:article-detail', kwargs={'uuid': self.uuid})

    def get_records_url(self):
        return reverse('media:article-records', kwargs={'uuid': self.uuid})


@wish_ext.OrderModel
@taggable('order')
class PurchaseBase(OrderBase):
    class Meta:
        indexes = [
            models.Index(fields=['datasource', ]),

            models.Index(fields=['team', 'datasource']),
        ]

    STATUS_CONFIRMED = 'CONFIRMED'
    STATUS_ABANDONED = 'ABANDONED'
    STATUS_KEEP = 'KEEP'

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    brand = models.ForeignKey(Brand, blank=False, null=True, on_delete=models.CASCADE)
    total_price = models.FloatField(default=0)

    datasource = models.ForeignKey(DataSource, blank=False, default=1, on_delete=models.CASCADE)

    attributions = JSONField(default=dict)
    categories = models.ManyToManyField(ProductCategory, blank=True)
    status = models.CharField(max_length=64, blank=False, null=False, default=STATUS_CONFIRMED)


class OrderProduct(BaseModel):

    class Meta:
        indexes = [
            models.Index(fields=['team', ]),
            models.Index(fields=['productbase', ]),
            models.Index(fields=['purchasebase', ]),
            models.Index(fields=['clientbase', ]),

            models.Index(fields=['team', 'productbase']),
            models.Index(fields=['team', 'purchasebase']),
            models.Index(fields=['team', 'clientbase']),
        ]

    team = models.ForeignKey(Team, blank=False, on_delete=models.CASCADE)
    datalist_id = models.IntegerField(blank=True, null=True)  # so we can trace back to it's original orderlist

    productbase = models.ForeignKey(RetailProduct, blank=False, null=False, on_delete=models.CASCADE)
    purchasebase = models.ForeignKey(PurchaseBase, blank=False, null=False, on_delete=models.CASCADE)
    clientbase = models.ForeignKey(ClientBase, blank=False, null=True, default=None, on_delete=models.CASCADE)

    refound = models.BooleanField(default=False)

    sale_price = models.FloatField(default=0.0)
    quantity = models.IntegerField(default=0)
    total_price = models.FloatField(default=0.0)


@client_info_model
class RetailInfo(BaseModel):

    clientbase = models.OneToOneField(ClientBase, related_name='media_info', blank=False, on_delete=models.CASCADE)

    # aggregated data, for performance purposes
    reading_rank = models.IntegerField(default=0)
    article_count = models.IntegerField(default=0)
    last_read_datetime = models.DateTimeField(null=True, blank=True)
    avg_reading_progress = models.FloatField(default=0)

    def __getattr__(self, attr):
        if attr == 'readbase_set':
            return TeamMongoDB(self.clientbase.team).readbases.filter(clientbase_id=self.clientbase_id)

    def get_sum_of_total_read(self):
        qs = self.clientbase.media_info.readbase_set

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)
        if total_progress is None:
            total_progress = 0

        return total_progress

    def get_count_of_total_article(self):
        return self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).values('path').count()

    def get_avg_of_each_read(self):
        qs = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None)
        if not qs.count():
            return 0

        data = qs.aggregate(total_progress=Sum('progress'))
        total_progress = data.get('total_progress', 0)

        return total_progress / qs.count()

    def get_times_of_read(self):
        return self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).count()

    def first_read(self):
        first_readbase = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').first()
        if first_readbase:
            return first_readbase['datetime']
        else:
            return None

    def last_read(self):
        last_readbase = self.clientbase.media_info.readbase_set.filter(F('articlebase_id') != None).order_by('datetime').last()
        if last_readbase:
            return last_readbase['datetime']
        else:
            return None
