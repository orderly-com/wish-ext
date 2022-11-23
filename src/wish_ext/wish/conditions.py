from typing import Any, Tuple

from django.utils import timezone
from django.db.models.expressions import OuterRef, Subquery
from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg, F, IntegerField
from django.contrib.postgres.aggregates import ArrayAgg
from django.contrib.postgres.fields import ArrayField

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition
from filtration.registries import condition
from filtration.exceptions import UseIdList

from tag_assigner.models import ValueTag

from cerem.tasks import aggregate_from_cerem

from .models import EventBase, LevelLogBase, EventLogBase, EventBase


class EventConditionBase(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_options(date_range=DateRangeCondition('日期區間'))

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        date_range = self.options.get('date_range')
        client_qs = client_qs.annotate(
            event_ids=Subquery(
                EventLogBase.objects.filter(clientbase_id=OuterRef('id'), datetime__range=date_range)
                .annotate(event_ids=ArrayAgg('event_id'))
                .values('event_ids')[:1], output_field=ArrayField(IntegerField())
            ),
        )

        if self.options.get('intersection', False):
            q = Q(event_ids__contains=choices)
        else:
            q = Q(event_ids__overlap=choices)

        return client_qs, q


@condition('免費活動', tab='活動')
class FreeEventCondition(EventConditionBase):
    def lazy_init(self, team, *args, **kwargs):
        events = team.eventbase_set.filter(cost_type=EventBase.COST_TYPE_FREE)

        data = list(events.values('id', text=F('name')))

        self.choice(*data)


@condition('兌換碼活動', tab='活動')
class CodeEventCondition(EventConditionBase):
    def lazy_init(self, team, *args, **kwargs):
        events = team.eventbase_set.filter(cost_type=EventBase.COST_TYPE_CODE)

        data = list(events.values('id', text=F('name')))

        self.choice(*data)


@condition('點數活動', tab='活動')
class CreditEventCondition(EventConditionBase):
    def lazy_init(self, team, *args, **kwargs):
        events = team.eventbase_set.filter(cost_type=EventBase.COST_TYPE_CREDIT)

        data = list(events.values('id', text=F('name')))

        self.choice(*data)


class PointConditionBase(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_options(date_range=DateRangeCondition('日期區間'))

    log_filter = Q()
    def lazy_init(self, team, *args, **kwargs):
        points = team.pointlogbase_set.filter(removed=False)

        point_names = list(points.values_list('point_name', flat=True))
        data = [{'id': point_name, 'text': point_name} for point_name in point_names]

        self.choice(*data)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        date_range = self.options.get('date_range')
        log_filter = self.log_filter
        if date_range:
            log_filter &= Q(pointlogbase__datetime__range=date_range)
        client_qs = client_qs.annotate(point_names=ArrayAgg('pointlogbase__point_name', filter=log_filter))

        if self.options.get('intersection', False):
            q = Q(point_names__contains=choices)
        else:
            q = Q(point_names__overlap=choices)

        return client_qs, q


@condition('兌點', tab='點數')
class PointUseCondition(PointConditionBase):
    log_filter = Q(pointlogbase__amount__lt=0)


@condition('給點', tab='點數')
class PointClaimCondition(PointConditionBase):
    log_filter = Q(pointlogbase__amount__gt=0)


@condition('等級', tab='等級')
class LevelCondition(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.options['intersection']

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        from_level = self.options.get('from_level')
        q = Q(current_level_id__in=choices)
        client_qs = client_qs.annotate(
            current_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=now).order_by('-from_datetime').values('to_level_id')[:1]
            ),
            previous_level_id=Subquery(
                LevelLogBase.objects.filter(clientbase_id=OuterRef('id'), from_datetime__lt=now).order_by('-from_datetime').values('from_level_id')[:1]
            )
        )
        if from_level:
            q &= Q(previous_level_id__in=from_level)


        return client_qs, q

    def real_time_init(self, team):
        from_level = SelectCondition('前次等級')
        self.add_options(from_level=from_level)
        levels = team.memberlevelbase_set.filter(removed=False)

        data = list(levels.values('id', text=F('name')))

        self.choice(*data)
        from_level.choice(*data)


class TicketTypeConditionBase(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_options(date_range=DateRangeCondition('日期區間'))
        del self.options['intersection']

    def get_eventlog_filter(self, choices):
        date_range = self.options.get('date_range')
        eventlog_filter = Q(eventlogbase__event__ticket_type__in=choices, eventlogbase__action=self.action)
        if date_range:
            eventlog_filter &= Q(eventlogbase__datetime__range=date_range)
        return eventlog_filter

    def lazy_init(self, team, *args, **kwargs):
        ticket_types = team.eventbase_set.filter(removed=False).values_list('ticket_type', flat=True).distinct()

        data = [{'id': type_name, 'text': type_name} for type_name in ticket_types]

        self.choice(*data)


class TicketNameConditionBase(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_options(date_range=DateRangeCondition('日期區間'))
        del self.options['intersection']

    def get_eventlog_filter(self, choices):
        eventlog_filter = Q(eventlogbase__event__ticket_type__in=choices, eventlogbase__action=self.action)
        if date_range:
            eventlog_filter &= Q(eventlogbase__datetime__range=date_range)
        return eventlog_filter

    def lazy_init(self, team, *args, **kwargs):
        ticket_names = team.eventbase_set.filter(removed=False).values_list('ticket_name', flat=True).distinct()

        data = [{'id': ticket_name, 'text': ticket_name} for ticket_name in ticket_names]

        self.choice(*data)

@condition('發券_票券類型', tab='票券')
class TicketTypeClaim(TicketTypeConditionBase):
    action = EventLogBase.ACTION_CLAIM
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        date_range = self.options.get('date_range')
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_type_event_count=Count(
                'eventlogbase',
                filter=self.get_eventlog_filter(choices)
            )
        )
        q = Q(claim_ticket_type_event_count__gt=0)

        return client_qs, q


@condition('發券_票券名稱', tab='票券')
class TicketNameClaim(TicketNameConditionBase):
    action = EventLogBase.ACTION_CLAIM
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_name_event_count=Count(
                'eventlogbase',
                filter=self.get_eventlog_filter(choices)
            )
        )
        q = Q(claim_ticket_name_event_count__gt=0)

        return client_qs, q

@condition('核銷_票券類型', tab='票券')
class TicketTypeUse(TicketTypeConditionBase):
    action = EventLogBase.ACTION_USE
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_type_event_count=Count(
                'eventlogbase',
                filter=self.get_eventlog_filter(choices)
            )
        )
        q = Q(claim_ticket_type_event_count__gt=0)

        return client_qs, q


@condition('核銷_票券名稱', tab='票券')
class TicketNameUse(TicketNameConditionBase):
    action = EventLogBase.ACTION_USE
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_name_event_count=Count(
                'eventlogbase',
                filter=self.get_eventlog_filter(choices)
            )
        )
        q = Q(claim_ticket_name_event_count__gt=0)

        return client_qs, q
