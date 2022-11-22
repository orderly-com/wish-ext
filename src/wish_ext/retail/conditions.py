from typing import Any, Tuple

from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg, F

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition
from filtration.registries import condition
from filtration.exceptions import UseIdList

from tag_assigner.models import ValueTag

from cerem.tasks import aggregate_from_cerem

@condition('品牌名稱', tab='訂單記錄')
class Brands(SelectCondition):
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

    def lazy_init(self, team, *args, **kwargs):
        brands = team.brand_set.filter(removed=False).order_by('order').values('id', text=F('name'))

        data = list(brands)

        self.choice(*data)

