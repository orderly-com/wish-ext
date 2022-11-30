import datetime
from dateutil import parser

from django.conf import settings
from django.db.models import Min

from importly.importers import DataImporter
from importly.formatters import (
    Formatted, format_datetime, format_int, format_bool
)

from datahub.data_flows import handle_data
from datahub.models import Field, FieldGroup, ChoiceField, PrimaryField

from ..wish.datahub import DataTypeLevel, DataTypeLevelLog, DataTypeEvent, DataTypeEventLog, DataTypePointLog
from ..wish.models import EventBase, MemberLevelBase, LevelLogBase, EventLogBase, PointLogBase

from .formatters import format_dict
from .models import Level, LevelLog, Event, EventLog, PointLog


class LevelImporter(DataImporter):

    data_type = DataTypeLevel

    class DataTransfer:
        class LevelTransfer:
            model = Level

            external_id = Formatted(str, 'id')

            name = Formatted(str, 'name')
            rank = Formatted(lambda x: format_int(x, default=0), 'rank')
            attributions = Formatted(dict, 'attributions')

    group_level = FieldGroup(key='LEVEL', name='等級')

    id = PrimaryField('等級 ID', required=True, group=group_level)

    name = Field('等級名稱', group=group_level)
    rank = Field('階級數字', group=group_level)
    attributions = Field('等級屬性', group=group_level, is_attributions=True)

    def process_raw_records(self):

        level_map = {}
        for level in self.team.memberlevelbase_set.values('id', 'external_id', 'rank', 'name', 'attributions'):
            level_map[level['external_id']] = MemberLevelBase(**level)

        levels = self.datalist.level_set.values('external_id', 'rank', 'name', 'attributions')
        levels_to_create = []
        levels_to_update = set()
        for level in levels:
            external_id = level['external_id']
            if external_id in level_map:
                levelbase = level_map[external_id]
                if levelbase.id:
                    levels_to_update.add(levelbase)
                levelbase.attributions.update(level['attributions'])
                levelbase.rank = level['rank']
                levelbase.name = level['name']
            else:
                level = MemberLevelBase(**level, team_id=self.team.id)
                levels_to_create.append(level)
                level_map[external_id] = level
        update_fields = ['rank', 'name', 'attributions']
        MemberLevelBase.objects.bulk_create(levels_to_create, batch_size=settings.BATCH_SIZE_M)
        MemberLevelBase.objects.bulk_update(levels_to_update, update_fields, batch_size=settings.BATCH_SIZE_M)


class LevelLogImporter(DataImporter):

    data_type = DataTypeLevelLog

    class DataTransfer:
        class LevelLogTransfer:
            model = LevelLog

            external_id = Formatted(str, 'id')

            from_level_id = Formatted(str, 'from_level_id')
            to_level_id = Formatted(str, 'to_level_id')
            clientbase_external_id = Formatted(str, 'member_id')
            datetime = Formatted(format_datetime, 'datetime')
            from_datetime = Formatted(format_datetime, 'from_datetime')
            to_datetime = Formatted(format_datetime, 'to_datetime')
            attributions = Formatted(dict, 'attributions')

    group_level_log = FieldGroup(key='LEVELLOG', name='等級記錄')

    id = PrimaryField('記錄ID', required=True, group=group_level_log)
    from_level_id = Field('原始等級編號', group=group_level_log)
    to_level_id = Field('後來等級編號', group=group_level_log)
    member_id = Field('會員ID', group=group_level_log)
    datetime = Field('建立時間', group=group_level_log)
    from_datetime = Field('等級開始時間', group=group_level_log)
    to_datetime = Field('等級到期時間', group=group_level_log)
    attributions = Field('等級記錄屬性', group=group_level_log, is_attributions=True)

    def process_raw_records(self):

        level_name_map = {}
        level_map = {}
        for external_id, name, level_id in self.team.memberlevelbase_set.values_list('external_id', 'name', 'id'):
            level_map[external_id] = level_id
            level_name_map[name] = level_id

        clientbase_map = {}
        for clientbase_id, external_id in self.team.clientbase_set.filter(removed=False).values_list('id', 'external_id'):
            clientbase_map[external_id] = clientbase_id

        logs = self.datalist.levellog_set.values(
            'from_level_id', 'to_level_id', 'datetime',
            'clientbase_external_id', 'attributions', 'from_datetime', 'to_datetime'
        )
        logs_to_create = []
        for log in logs:
            from_level_id = log.pop('from_level_id')
            to_level_id = log.pop('to_level_id')
            clientbase_external_id = log.pop('clientbase_external_id')
            log['from_level_id'] = level_map.get(from_level_id, level_name_map.get(from_level_id))
            log['to_level_id'] = level_map.get(to_level_id, level_name_map.get(to_level_id))
            clientbase_id = clientbase_map.get(clientbase_external_id)
            if not clientbase_id or not log['from_level_id'] or not log['to_level_id']:
                continue
            logs_to_create.append(LevelLogBase(**log, clientbase_id=clientbase_id, team_id=self.team.id))

        LevelLogBase.objects.bulk_create(logs_to_create, batch_size=settings.BATCH_SIZE_M)


class EventImporter(DataImporter):

    data_type = DataTypeEvent

    class DataTransfer:
        class EventTransfer:
            model = Event

            external_id = Formatted(str, 'id')

            name = Formatted(str, 'name')
            ticket_type = Formatted(str, 'ticket_type')
            ticket_name = Formatted(str, 'ticket_name')
            cost_type = Formatted(str, 'cost_type')
            attributions = Formatted(dict, 'attributions')

    group_event = FieldGroup(key='EVENT', name='活動')

    id = Field('活動編號', group=group_event)
    name = Field('活動名稱', group=group_event)
    ticket_type = Field('票券類型', group=group_event)
    ticket_name = Field('票券名稱', group=group_event)

    COST_TYPE_CHOICES = {
        EventBase.COST_TYPE_FREE: '免費',
        EventBase.COST_TYPE_CODE: '兌換碼',
        EventBase.COST_TYPE_CREDIT: '點數',
    }
    cost_type = ChoiceField('免費/點數/兌換碼', group=group_event, choices=COST_TYPE_CHOICES)
    attributions = Field('活動屬性', group=group_event, is_attributions=True)

    def process_raw_records(self):

        event_map = {}
        for event in self.team.eventbase_set.values('id', 'external_id', 'ticket_type', 'name', 'cost_type', 'attributions', 'ticket_name'):
            event_map[event['external_id']] = EventBase(**event)

        events = self.datalist.event_set.values('external_id', 'ticket_type', 'name', 'attributions', 'cost_type', 'ticket_name')
        events_to_create = []
        events_to_update = set()
        for event in events:
            external_id = event['external_id']
            if external_id in event_map:
                eventbase = event_map[external_id]
                if eventbase.id:
                    events_to_update.add(eventbase)
                eventbase.attributions.update(event['attributions'])
                eventbase.ticket_type = event['ticket_type']
                eventbase.ticket_name = event['ticket_name']
                eventbase.name = event['name']
                eventbase.cost_type = event['cost_type']
            else:
                event = EventBase(**event, team_id=self.team.id)
                events_to_create.append(event)
                event_map[external_id] = event
        update_fields = ['ticket_type', 'name', 'attributions', 'cost_type', 'ticket_name']
        EventBase.objects.bulk_create(events_to_create, batch_size=settings.BATCH_SIZE_M)
        EventBase.objects.bulk_update(events_to_update, update_fields, batch_size=settings.BATCH_SIZE_M)


class EventLogImporter(DataImporter):

    data_type = DataTypeEventLog

    class DataTransfer:
        class EventLogTransfer:
            model = EventLog

            external_id = Formatted(str, 'id')

            event_external_id = Formatted(str, 'event_id')
            clientbase_external_id = Formatted(str, 'member_id')
            action = Formatted(str, 'action')
            datetime = Formatted(format_datetime, 'datetime')
            attributions = Formatted(dict, 'attributions')

    group_event_log = FieldGroup(key='EVENTLOG', name='活動記錄')

    id = PrimaryField('記錄編號', required=True, group=group_event_log)
    event_id = Field('活動編號', group=group_event_log)
    member_id = Field('會員ID', group=group_event_log)
    datetime = Field('時間', group=group_event_log)

    ACTION_CHOIES = {
        EventLog.ACTION_CLAIM: '領取',
        EventLog.ACTION_USE: '使用'
    }

    action = ChoiceField('領取/使用', group=group_event_log, choices=ACTION_CHOIES)
    attributions = Field('活動記錄屬性', group=group_event_log, is_attributions=True)

    def process_raw_records(self):

        event_map = {}
        for external_id, event_id in self.team.eventbase_set.values_list('external_id', 'id'):
            event_map[external_id] = event_id

        clientbase_map = {}
        for clientbase_id, external_id in self.team.clientbase_set.filter(removed=False).values_list('id', 'external_id'):
            clientbase_map[external_id] = clientbase_id

        logs = self.datalist.eventlog_set.values(
            'event_external_id', 'action', 'datetime',
            'clientbase_external_id', 'attributions', 'external_id'
        )
        logs_to_create = []
        for log in logs:
            event_id = log.pop('event_external_id')
            clientbase_external_id = log.pop('clientbase_external_id')
            log['event_id'] = event_map.get(event_id)
            if not log['external_id']:
                del log['external_id']
            clientbase_id = clientbase_map.get(clientbase_external_id)
            print(event_map, event_id, log['event_id'])

            if not clientbase_id or not log['event_id']:
                continue
            logs_to_create.append(EventLogBase(**log, clientbase_id=clientbase_id, team_id=self.team.id))

        EventLogBase.objects.bulk_create(logs_to_create, batch_size=settings.BATCH_SIZE_M, ignore_conflicts=True)


class PointLogImporter(DataImporter):

    data_type = DataTypePointLog

    class DataTransfer:
        class PointLogTransfer:
            model = PointLog

            external_id = Formatted(str, 'id')

            point_name = Formatted(str, 'point_name')
            clientbase_external_id = Formatted(str, 'member_id')
            amount = Formatted(format_int, 'amount')
            is_transaction = Formatted(format_bool, 'is_transaction')
            datetime = Formatted(format_datetime, 'datetime')
            attributions = Formatted(dict, 'attributions')

    group_event_log = FieldGroup(key='POINTLOG', name='點數記錄')

    id = PrimaryField('記錄編號', required=True, group=group_event_log)
    point_name = Field('點數名稱', group=group_event_log)
    member_id = Field('會員ID', group=group_event_log)
    datetime = Field('時間', group=group_event_log, required=True)
    amount = Field('數量', group=group_event_log)
    attributions = Field('點數記錄屬性', group=group_event_log, is_attributions=True)

    is_transaction = Field('交易/非交易', group=group_event_log)

    def process_raw_records(self):

        pointlog_map = {}
        for log in self.team.pointlogbase_set.values(
            'id', 'external_id', 'point_name',
            'datetime', 'amount', 'attributions', 'is_transaction'
        ):
            pointlog_map[log['external_id']] = PointLogBase(**log)

        clientbase_map = {}
        for clientbase_id, external_id in self.team.clientbase_set.filter(removed=False).values_list('id', 'external_id'):
            clientbase_map[external_id] = clientbase_id

        logs = self.datalist.pointlog_set.values(
            'external_id', 'point_name', 'clientbase_external_id', 'datetime',
            'amount', 'attributions', 'is_transaction'
        )
        logs_to_create = []
        logs_to_update = set()
        for log in logs:
            external_id = log['external_id']
            clientbase_external_id = log.pop('clientbase_external_id')
            if external_id in pointlog_map:
                logbase = pointlog_map[external_id]
                if logbase.id:
                    logs_to_update.add(logbase)
                logbase.attributions.update(log['attributions'])
                logbase.point_name = log['point_name']
                logbase.datetime = log['datetime']
                logbase.is_transaction = log['is_transaction']
                logbase.amount = log['amount']
            else:
                clientbase_id = clientbase_map.get(clientbase_external_id)
                if not clientbase_id:
                    continue
                log = PointLogBase(**log, team_id=self.team.id, clientbase_id=clientbase_id)
                logs_to_create.append(log)
                pointlog_map[external_id] = log
        update_fields = ['point_name', 'attributions', 'amount', 'is_transaction', 'datetime']
        PointLogBase.objects.bulk_create(logs_to_create, batch_size=settings.BATCH_SIZE_M)
        PointLogBase.objects.bulk_update(logs_to_update, update_fields, batch_size=settings.BATCH_SIZE_M)
