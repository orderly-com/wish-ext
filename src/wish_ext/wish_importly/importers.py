import datetime
from dateutil import parser

from django.conf import settings
from django.db.models import Min

from importly.importers import DataImporter
from importly.formatters import (
    Formatted, format_datetime, format_int
)

from datahub.data_flows import handle_data
from datahub.models import Field, FieldGroup, ChoiceField, PrimaryField

from ..wish.datahub import DataTypeLevel, DataTypeLevelLog, DataTypeEvent, DataTypeEventLog
from ..wish.models import EventBase, MemberLevelBase, LevelLogBase, EventLogBase

from .formatters import format_dict
from .models import Level, LevelLog, Event, EventLog


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

    id = PrimaryField('文章編號', required=True, group=group_level)

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
                levelbase.name = levelbase['name']
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
    from_level_id = Field('來源等級編號', group=group_level_log)
    to_level_id = Field('現在等級編號', group=group_level_log)
    member_id = Field('會員ID', group=group_level_log)
    datetime = Field('建立時間', group=group_level_log)
    from_datetime = Field('等級開始時間', group=group_level_log)
    to_datetime = Field('等級到期時間', group=group_level_log)
    attributions = Field('等級記錄屬性', group=group_level_log, is_attributions=True)


class EventImporter(DataImporter):

    data_type = DataTypeEvent

    class DataTransfer:
        class EventTransfer:
            model = Event

            external_id = Formatted(str, 'id')

            name = Formatted(str, 'name')
            ticket_type = Formatted(str, 'ticket_type')
            cost_type = Formatted(str, 'cost_type')
            attributions = Formatted(dict, 'attributions')

    group_event = FieldGroup(key='EVENT', name='活動')

    id = PrimaryField('活動編號', required=True, group=group_event)

    name = Field('活動名稱', group=group_event)
    ticket_type = Field('票券類型', group=group_event)
    cost_type = Field('免費/點數/兌換碼', group=group_event)
    attributions = Field('等級屬性', group=group_event, is_attributions=True)

    def process_raw_records(self):

        event_map = {}
        for event in self.team.eventbase_set.values('id', 'external_id', 'ticket_type', 'name', 'cost_type', 'attributions'):
            event_map[event['external_id']] = MemberLevelBase(**event)

        events = self.datalist.event_set.values('external_id', 'ticket_type', 'name', 'attributions', 'cost_type')
        events_to_create = []
        events_to_update = set()
        for event in events:
            external_id = event['external_id']
            if external_id in event_map:
                eventbase = event_map[external_id]
                if event.id:
                    events_to_update.add(eventbase)
                eventbase.attributions.update(event['attributions'])
                eventbase.ticket_type = event['ticket_type']
                eventbase.name = eventbase['name']
                eventbase.cost_type = eventbase['cost_type']
            else:
                event = EventBase(**event, team_id=self.team.id)
                events_to_create.append(event)
                event_map[external_id] = event
        update_fields = ['ticket_type', 'name', 'attributions', 'cost_type']
        EventBase.objects.bulk_create(events_to_create, batch_size=settings.BATCH_SIZE_M)
        EventBase.objects.bulk_update(events_to_update, update_fields, batch_size=settings.BATCH_SIZE_M)


class EventLogImporter(DataImporter):

    data_type = DataTypeLevelLog

    class DataTransfer:
        class EventLogTransfer:
            model = LevelLog

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
    action = Field('領取/使用', group=group_event_log)
    attributions = Field('活動記錄屬性', group=group_event_log, is_attributions=True)
