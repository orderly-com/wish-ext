import re

from datahub.models import DataType, data_type
from django.db.models import Min, Max, F

from ..extension import wish_ext

@data_type
class DataTypeOrder(DataType):
    key = 'order'
    name = '訂單'
    detail_availabel = True

    @staticmethod
    def get_records_fields_display():
        '''
        define the datatable of datalist detail page,
        each field must be as the following format:
        (field, display_name, col-width, additional_class)
        '''
        return [
            ('_id', '編號', '3', 'text-center'),
            ('datetime', '時間', '3', 'text-center'),
            ('total_price', '總金額', '3', 'text-center'),
            ('client_id', '會員編號', '3', 'text-center'),
        ]

    @staticmethod
    def get_datetime_min(datalist):
        aggregation = datalist.order_set.aggregate(min_datetime=Min('datetime'))
        return aggregation['min_datetime']

    @staticmethod
    def get_datetime_max(datalist):
        aggregation = datalist.order_set.aggregate(max_datetime=Max('datetime'))
        return aggregation['max_datetime']

    @staticmethod
    def get_records(datalist):
        records = list(datalist.datalistrow_set.values(
            _id=F('order__external_id'),
            datetime=F('order__datetime'),
            total_price=F('order__purchasebase__total_price'),
            client_id=F('client__external_id')
        ))
        for item in records:
            try:
                item['datetime'] = item['datetime'].strftime('%Y/%m/%d %H:%M:%S')
            except:
                item['datetime'] = '-'
        return records

@data_type
class DataTypeProduct(DataType):
    key = 'product'
    name = '商品'
    color = 'blue'

    @staticmethod
    def get_datetime_min(datalist):
        aggregation = datalist.article_set.aggregate(min_datetime=Min('datetime'))
        return aggregation['min_datetime']

    @staticmethod
    def get_datetime_max(datalist):
        aggregation = datalist.article_set.aggregate(max_datetime=Max('datetime'))
        return aggregation['max_datetime']


class DataTypeSyncReadingData(DataType):
    key = 'sync_reading_data'


class channels:
    ARTICLE_IMPORT = 'article_import'
    ARTICLE_TO_ARTICLEBASE = 'article_to_articlebase'
