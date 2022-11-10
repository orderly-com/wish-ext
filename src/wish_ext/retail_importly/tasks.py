from django.conf import settings
from django.core.exceptions import ValidationError

from datahub.models import DataSource
from importly.exceptions import EssentialDataMissing
from importly.models import DataList

from config.celery import app
from team.models import Team

from .importers import OrderImporter


def process_datalist(team_slug, data, importer_cls):
    team = Team.objects.get(slug=team_slug)

    datasource = data.get('datasource')

    try:
        datasource = DataSource.objects.filter(uuid=datasource).only('id').first()
    except (DataSource.DoesNotExist, ValidationError):
        raise EssentialDataMissing('datasource')

    try:
        rows = data['data']
    except KeyError:
        raise EssentialDataMissing('data')

    importer = importer_cls(team, datasource)

    datalist = importer.create_datalist(rows)
    datalist.set_step(DataList.STEP_CREATE_RAW_RECORDS)

    importer.data_to_raw_records()
    datalist.set_step(DataList.STEP_PROCESS_RAW_RECORDS)

    importer.process_raw_records()
    datalist.set_step(DataList.STEP_DONE)



@app.task(time_limit=settings.APP_TASK_TIME_LIMIT_SM)
def process_orderlist(team_slug, data):
    cleaned_data = []
    for row in data['data']:
        for line_item in row.pop('items', []):
            row = row.copy()
            row.update(line_item)
            cleaned_data.append(row)
    data['data'] = cleaned_data
    print(data['data'])
    process_datalist(team_slug, data, OrderImporter)
