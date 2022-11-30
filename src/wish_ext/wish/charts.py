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
from cdp.web.orderly_core.team import overview_charts, LevelUpMatrMatrix, AttributionPieChart
from cdp.web.charts.drawers import MatrixChart
from cdp.web.charts.registries import chart_category, dashboard_preset


@overview_charts.chart(name='等級升降續熱區圖')
class LevelUpMatrMatrix(MatrixChart):
    def draw(self):
        data = [
            {'x': '初始', 'y': '一般會員', 'v': 0},
            {'x': '升等', 'y': '一般會員', 'v': 0.2},
            {'x': '初始', 'y': '銅等會員', 'v': 0.6},
            {'x': '升等', 'y': '銅等會員', 'v': 1.0},
            {'x': '降等', 'y': '一般會員', 'v': 0.2},
            {'x': '降等', 'y': '銅等會員', 'v': 0.6},
            {'x': '降等', 'y': '銀等會員', 'v': 1.0},
            {'x': '初始', 'y': '銀等會員', 'v': 0.2},
            {'x': '升等', 'y': '銀等會員', 'v': 0.4},
            {'x': '續等', 'y': '一般會員', 'v': 0.6},
            {'x': '續等', 'y': '銅等會員', 'v': 1.0},
            {'x': '續等', 'y': '銀等會員', 'v': 0.2},
            {'x': '降等', 'y': '金等會員', 'v': 1.0},
            {'x': '初始', 'y': '金等會員', 'v': 0.2},
            {'x': '升等', 'y': '金等會員', 'v': 0.4},
            {'x': '續等', 'y': '金等會員', 'v': 0.6},
        ]
        for item in data:
            self.set_value(item['x'], item['y'], item['v'])

@dashboard_preset
class Levels:
    name = '等級模組'
    charts = [
        AttributionPieChart.preset('等級人數圓餅圖'),
        LevelUpMatrMatrix.preset('等級升降熱區圖'),
    ]
