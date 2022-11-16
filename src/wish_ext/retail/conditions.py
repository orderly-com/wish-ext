from typing import Any, Tuple

from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg, F

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition
from filtration.registries import condition
from filtration.exceptions import UseIdList

from tag_assigner.models import ValueTag

from cerem.tasks import aggregate_from_cerem
