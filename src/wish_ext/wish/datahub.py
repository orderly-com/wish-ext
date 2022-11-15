import re

from datahub.models import DataType, data_type

from ..extension import wish_ext


@data_type
class DataTypeLevel(DataType):
    key = 'event'
    name = '等級'


@data_type
class DataTypeLevelLog(DataType):
    key = 'level_log'
    name = '等級記錄'


@data_type
class DataTypeEvent(DataType):
    key = 'event'
    name = '活動'


@data_type
class DataTypeEventLog(DataType):
    key = 'event_log'
    name = '活動記錄'


@data_type
class DataTypePointLog(DataType):
    key = 'point_log'
    name = '點數記錄'
