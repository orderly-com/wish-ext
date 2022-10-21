import datetime
import itertools
import statistics
from dateutil import relativedelta

from django.utils import timezone
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast
from django.db.models import Count, Func, Max, Min, IntegerField

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart, HorizontalBarChart, DataCard
from filtration.conditions import DateRangeCondition, ModeCondition
from orderly_core.team.charts import client_behavior_charts
from cerem.tasks import clickhouse_client
from cerem.utils import F
