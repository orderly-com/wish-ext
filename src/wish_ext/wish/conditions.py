from typing import Any, Tuple

from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg, F

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition
from filtration.registries import condition
from filtration.exceptions import UseIdList

from tag_assigner.models import ValueTag

from cerem.tasks import aggregate_from_cerem

from .models import EventBase


class EventConditionBase(SelectCondition):
    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:
        client_qs = client_qs.annotate(event_ids=ArrayAgg('eventlogbase__eventbase_id'))

        if self.options.get('intersection', False):
            q &= Q(event_ids__contains=choices)
        else:
            q &= Q(event_ids__overlap=choices)

        return client_qs, q


@condition
class FreeEventCondition(EventConditionBase):
    def lazy_init(self, team, *args, **kwargs):
        events = team.eventbase_set.filter(cost_type=EventBase.COST_TYPE_FREE)

        data = list(events.values('id', text=F('name')))

        self.choice(*data)


@condition
class CodeEventCondition(EventConditionBase):
    def lazy_init(self, team, *args, **kwargs):
        events = team.eventbase_set.filter(cost_type=EventBase.COST_TYPE_CODE)

        data = list(events.values('id', text=F('name')))

        self.choice(*data)


@condition
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


@condition
class PointUseCondition(PointConditionBase):
    log_filter = Q(pointlogbase__amount__lt=0)


@condition
class PointClaimCondition(PointConditionBase):
    log_filter = Q(pointlogbase__amount__gt=0)


@condition
class LevelCondition(SelectCondition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.options['intersection']

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:

        q = Q(wishinfo__level_id__in=choices)

        return client_qs, q

    def lazy_init(self, team, *args, **kwargs):
        levels = team.memberlevelbase_set.filter(removed=False)

        data = list(levels.values('id', text=F('name')))

        self.choice(*data)


@condition
class LevelDirectionCondition(SelectCondition):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.options['intersection']
        self.choice(
            {'id': WishInfo.LEVEL_UP, 'text': '升階'},
            {'id': WishInfo.LEVEL_STAY, 'text': '續等'},
            {'id': WishInfo.LEVEL_DOWN, 'text': '降等'},
        ).default(LEV_UP)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:

        q = Q(wishinfo__last_level_direction__in=choices)

        return client_qs, q


class TicketTypeConditionBase(SelectCondition):

    def lazy_init(self, team, *args, **kwargs):
        ticket_types = team.eventbase_set.filter(removed=False).values_list('ticket_type', flat=True).distinct()

        data = [{'id': type_name, 'name': type_name} for type_name in ticket_types]

        self.choice(*data)


class TicketNameConditionBase(SelectCondition):

    def lazy_init(self, team, *args, **kwargs):
        ticket_names = team.eventbase_set.filter(removed=False).values_list('ticket_name', flat=True).distinct()

        data = [{'id': ticket_name, 'name': ticket_name} for ticket_name in ticket_names]

        self.choice(*data)
