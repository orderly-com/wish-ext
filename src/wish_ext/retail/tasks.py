import numpy as np
import pandas as pd
import math

from django.conf import settings
from django.core.exceptions import ValidationError
from django_pandas.io import read_frame

from datahub.models import DataSource
from importly.models import DataList
from importly.exceptions import EssentialDataMissing

from config.celery import app
from team.models import Team, ClientBase
from core.utils import run

from .models import PurchaseBase
from ..extension import wish_ext

@wish_ext.periodic_task()
def calculate_clientbase_rfm_for_team(team_id):
    '''
    updates ['avg_repurchase_days', 'recency', 'frequency', 'monetary', 'total_score', 'percentile']
    in Clientbase separated by team
    '''
    team = Team.objects.get(id=team_id)

    qs = team.purchasebase_set.select_related('clientbase') \
        .filter(
            status=PurchaseBase.STATUS_CONFIRMED,
            removed=False,
            datetime__gte=settings.FIRST_DATE,
            clientbase__internal_member=False,
            clientbase__removed=False)\
        .order_by('datetime')\
        .values('clientbase__id', 'datetime', 'total_price')

    df = read_frame(qs)

    if df.empty:
        return False

    df_first = df.drop_duplicates('clientbase__id', keep='first').set_index('clientbase__id')['datetime']
    df_last = df.drop_duplicates('clientbase__id', keep='last').set_index('clientbase__id')['datetime']
    df_purchase_count = df.groupby(by='clientbase__id').count().rename(columns={'datetime': 'purchase_count'})['purchase_count']
    df_purchase_amount = df.groupby(by='clientbase__id').sum()

    df = pd.merge(df_first, df_last, suffixes=('_first', '_last'), left_index=True, right_index=True).join(df_purchase_count).join(df_purchase_amount)

    labels = [1, 2, 3, 4, 5]

    # rate perchase count
    count_max, count_min = df['purchase_count'].max(), df['purchase_count'].min()
    count_gap = (count_max - count_min) / 4  # 20%

    if count_gap != 0:
        count_step = [-np.inf, 2, 4, 6, 8, np.inf]  # the count of purchase
        # count_step = [-np.inf] + list(np.arange(count_min, count_max, count_gap)) + [np.inf]
        df['frequency'] = pd.cut(df['purchase_count'], count_step, labels=labels)
    else:
        df['frequency'] = 1

    # rate purchase amount
    total_price_list = df['total_price'].values.tolist()
    total_price_list.sort()
    total_price_list = total_price_list[:math.ceil(len(total_price_list) * 0.95)]  # discard the top 0.5%
    amount_max, amount_min = max(total_price_list), min(total_price_list)
    amount_gap = math.ceil((amount_max - amount_min) / 6)
    amount_min = math.ceil(amount_min + amount_gap)  # shift amount_min
    amount_max = math.ceil(amount_max - amount_gap)  # shift amount_max

    if amount_gap != 0:
        amount_step = list(np.arange(amount_min, amount_max, amount_gap))[:4]  # take only 4 elements
        amount_step = [-np.inf] + amount_step + [np.inf]
        df['monetary'] = pd.cut(df['total_price'], amount_step, labels=labels)
    else:
        df['monetary'] = 1

    # rate purchase amount
    try:
        df['last_buy_recency'] = (pd.Timestamp.today('UTC') - df['datetime_last']).dt.days
        recency_max, recency_min = df['last_buy_recency'].max(), df['last_buy_recency'].min()
        recency_gap = (recency_max - recency_min) / 4
    except Exception:
        recency_gap = 0

    if recency_gap != 0:
        return_day = team.get_return_day()
        if return_day == 0:
            recency_step = [-np.inf] + list(np.arange(recency_min, recency_max, recency_gap)) + [np.inf]
        else:
            recency_step = [-np.inf, return_day, return_day * 1.5, return_day * 2.5, return_day * 3, np.inf]  # recency is based on return day
        df['recency'] = pd.cut(df['last_buy_recency'], recency_step, labels=labels)
    else:
        df['recency'] = 1

    # hot fixed
    df.replace({'recency': 5}, -5, inplace=True)
    df.replace({'recency': 4}, -4, inplace=True)
    df.replace({'recency': 3}, -3, inplace=True)
    df.replace({'recency': 2}, -2, inplace=True)
    df.replace({'recency': 1}, -1, inplace=True)

    df.replace({'recency': -5}, 1, inplace=True)
    df.replace({'recency': -4}, 2, inplace=True)
    df.replace({'recency': -3}, 3, inplace=True)
    df.replace({'recency': -2}, 4, inplace=True)
    df.replace({'recency': -1}, 5, inplace=True)

    # # rate date, fill no repurchase date to mean if only 1 purchase record, fill 0 if no purchase records found
    # #TODO Fix rfm's recency score
    df['time_gap'] = (df['datetime_last'] - df['datetime_first']).dt.days
    # df['last_buy_gap'] = (pd.Timestamp.today('UTC') - df['datetime_last']).dt.days

    df['avg_repurchase_days'] = (df['time_gap'] / (df['purchase_count'] - 1))
    df.fillna(value={'avg_repurchase_days': 0}, inplace=True)
    # df.loc[df['purchase_count'] == 1, ['avg_repurchase_days']] = 0

    # df['buy_gap_rate'] = df['last_buy_gap'] / df['avg_repurchase_days']
    # df['buy_gap_rate'] = df['buy_gap_rate'].fillna(0)
    # df.loc[df['purchase_count'] == 1, ['buy_gap_rate']] = 0
    # df['recency'] = pd.cut(df['buy_gap_rate'], [-np.inf, 0.5, 1, 1.5, 2, np.inf], labels=labels)

    # df['buy_gap_rate'].replace(np.inf, 0, inplace=True)
    df['avg_repurchase_days'].replace(np.inf, 0, inplace=True)
    # TODO Fix rfm's recency score

    for rate in ['recency', 'frequency', 'monetary']:
        df[rate] = df[rate].astype(int)
    df['total_score'] = df['recency'] + df['frequency'] + df['monetary']

    for rate in ['recency', 'frequency', 'monetary']:
        df[rate] = df[rate].astype(str)
    df['rfm_segment'] = df['recency'] + df['frequency'] + df['monetary']

    df.sort_values('total_score', inplace=True, ascending=False)
    df.reset_index(inplace=True)

    # give percentile from 1 to 10 for clients in a team
    df['percentile'] = 0
    step = df.shape[0] / 10
    if step != 0:
        start = 0
        for i in range(1, 11):
            stop = int(step * i)
            df.loc[start:stop, ('percentile')] = i
            start = stop

    df.set_index('clientbase__id', inplace=True)

    value_list = ['avg_repurchase_days', 'recency', 'frequency', 'monetary', 'total_score', 'percentile', 'rfm_segment']
    df_dict = df[value_list].to_dict()

    qs = team.clientbase_set.filter(removed=False, internal_member=False).values('id')

    items_to_update = []
    clientbases = list(qs)
    for item in clientbases:

        item_id = item['id']

        avg_repurchase_days = df_dict.get('avg_repurchase_days', dict).get(item_id, 0)
        rfm_recency = int(df_dict.get('recency', dict).get(item_id, 0))
        rfm_frequency = int(df_dict.get('frequency', dict).get(item_id, 0))
        rfm_monetary = int(df_dict.get('monetary', dict).get(item_id, 0))
        rfm_total_score = df_dict.get('total_score', dict).get(item_id, 0)
        rfm_percentile = df_dict.get('percentile', dict).get(item_id, 0)
        rfm_segment = df_dict.get('rfm_segment', dict).get(item_id)

        obj = ClientBase(
            id=item_id,
            avg_repurchase_days=avg_repurchase_days,
            rfm_recency=rfm_recency,
            rfm_frequency=rfm_frequency,
            rfm_monetary=rfm_monetary,
            rfm_total_score=rfm_total_score,
            rfm_percentile=rfm_percentile,
            rfm_segment=rfm_segment,
        )

        items_to_update.append(obj)

    clientbases = []

    # reset avg_repurchase_days & rfm_recency
    team.clientbase_set.update(avg_repurchase_days=-1, rfm_recency=-1)

    # update avg_repurchase_days & rfm_recency
    team.clientbase_set.bulk_update(items_to_update, ['avg_repurchase_days', 'rfm_recency'], batch_size=settings.BATCH_SIZE_L)

    # reset rfm_frequency &rfm_monetary
    team.clientbase_set.update(rfm_frequency=-1, rfm_monetary=-1)

    # update rfm_frequency &rfm_monetary
    team.clientbase_set.bulk_update(items_to_update, ['rfm_frequency', 'rfm_monetary'], batch_size=settings.BATCH_SIZE_L)

    # reset rfm_total_score & rfm_percentile
    team.clientbase_set.update(rfm_total_score=-1, rfm_percentile=-1)

    # update rfm_total_score & rfm_percentile
    team.clientbase_set.bulk_update(items_to_update, ['rfm_total_score', 'rfm_percentile'], batch_size=settings.BATCH_SIZE_L)

    # reset rfm_segment
    team.clientbase_set.update(rfm_segment=None)

    # update rfm_segment
    team.clientbase_set.bulk_update(items_to_update, ['rfm_segment'], batch_size=settings.BATCH_SIZE_L)



@wish_ext.periodic_task()
def calculate_rfm():
    for team_id in Team.objects.filter(removed=False).values_list('id', flat=True):
        run(calculate_clientbase_rfm_for_team, team_id)
