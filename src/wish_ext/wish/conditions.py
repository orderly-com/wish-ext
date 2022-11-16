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
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        client_qs = client_qs.annotate(
            event_ids=Subquery(
                EventLogBase.objects.filter(clientbase_id=OuterRef('id'))
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
    log_filter = Q()
    def lazy_init(self, team, *args, **kwargs):
        points = team.pointlogbase_set.filter(removed=False)

        data = list(points.values(id=F('point_name'), text=F('point_name')).unique())

        self.choice(*data)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        client_qs = client_qs.annotate(point_names=ArrayAgg('pointlogbase__point_name', filter=self.log_filter))

        if self.options.get('intersection', False):
            q &= Q(point_names__contains=choices)
        else:
            q &= Q(point_names__overlap=choices)

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
        del self.options['intersection']

    def lazy_init(self, team, *args, **kwargs):
        ticket_types = team.eventbase_set.filter(removed=False).values_list('ticket_type', flat=True).distinct()

        data = [{'id': type_name, 'text': type_name} for type_name in ticket_types]

        self.choice(*data)


class TicketNameConditionBase(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.options['intersection']

    def lazy_init(self, team, *args, **kwargs):
        ticket_names = team.eventbase_set.filter(removed=False).values_list('ticket_name', flat=True).distinct()

        data = [{'id': ticket_name, 'text': ticket_name} for ticket_name in ticket_names]

        self.choice(*data)

@condition('發券_票券類型', tab='票券')
class TicketTypeClaim(TicketTypeConditionBase):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_type_event_count=Count(
                'eventlogbase',
                filter=Q(eventlogbase__event__ticket_type__in=choices, eventlogbase__action=EventLogBase.ACTION_CLAIM)
            )
        )
        q = Q(claim_ticket_type_event_count__gt=0)

        return client_qs, q


@condition('發券_票券名稱', tab='票券')
class TicketNameClaim(TicketNameConditionBase):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_name_event_count=Count(
                'eventlogbase',
                filter=Q(eventlogbase__event__ticket_name__in=choices, eventlogbase__action=EventLogBase.ACTION_CLAIM)
            )
        )
        q = Q(claim_ticket_name_event_count__gt=0)

        return client_qs, q

@condition('核銷_票券類型', tab='票券')
class TicketTypeUse(TicketTypeConditionBase):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_type_event_count=Count(
                'eventlogbase',
                filter=Q(eventlogbase__event__ticket_type__in=choices, eventlogbase__action=EventLogBase.ACTION_USE)
            )
        )
        q = Q(claim_ticket_type_event_count__gt=0)

        return client_qs, q


@condition('核銷_票券名稱', tab='票券')
class TicketNameUse(TicketNameConditionBase):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        now = timezone.now()
        client_qs = client_qs.annotate(
            claim_ticket_name_event_count=Count(
                'eventlogbase',
                filter=Q(eventlogbase__event__ticket_name__in=choices, eventlogbase__action=EventLogBase.ACTION_USE)
            )
        )
        q = Q(claim_ticket_name_event_count__gt=0)

        return client_qs, q
