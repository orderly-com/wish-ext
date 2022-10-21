import re

from datahub.models import DataType, data_type

from ..extension import wish_ext


class DataTypeLevel(DataType):
    key = 'event'


class DataTypeLevelLog(DataType):
    key = 'level_log'


class DataTypeEvent(DataType):
    key = 'event'


class DataTypeEventLog(DataType):
    key = 'event_log'
