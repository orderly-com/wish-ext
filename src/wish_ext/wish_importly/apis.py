import orjson

from django.http import JsonResponse
from django.conf import settings

from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework import status

from external_app.models import ExternalAppApiKey

from ..extension import wish_ext
from .tasks import process_eventlist, process_levellist, process_eventloglist, process_levelloglist, process_pointloglist

class APIImportBaseView(APIView):
    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):

        signature = kwargs.get('signature')
        api_key = request.headers.get('X-Api-Key')

        if not any([signature, api_key]):
            return JsonResponse({'result': False, 'msg': {'title': 'Value Missing', 'text': 'Signature or api_key is missing.'}}, status=status.HTTP_400_BAD_REQUEST)

        team = ExternalAppApiKey.get_team(signature, api_key)

        if not team:
            return JsonResponse({'result': False, 'msg': {'title': 'Not Valid', 'text': 'api_key is not valid or is expired.'}}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            data = orjson.loads(request.body.decode('utf-8'))
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Data is not valid or is not well formated.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if 'datasource' not in data:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Datasource is missing.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if 'data' not in data:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Data is missing.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if len(data['data']) > settings.BATCH_SIZE_L:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': f'Max row of data per request is {settings.BATCH_SIZE_L}.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if settings.DEBUG is True:
            self.task(team_slug=team.slug, data=data)
        else:
            self.task.delay(team_slug=team.slug, data=data)

        return JsonResponse({'result': True, 'msg': {'title': 'OK', 'text': 'Data is recived'}}, status=status.HTTP_200_OK)


@wish_ext.api('v1/<signature>/eventlist/')
class ImportEventList(APIImportBaseView):
    task = process_eventlist


@wish_ext.api('v1/<signature>/eventloglist/')
class ImportEventLogList(APIImportBaseView):
    task = process_eventloglist


@wish_ext.api('v1/<signature>/levellist/')
class ImportLevelList(APIImportBaseView):
    task = process_levellist


@wish_ext.api('v1/<signature>/levelloglist/')
class ImportLevelLogList(APIImportBaseView):
    task = process_levelloglist


@wish_ext.api('v1/<signature>/pointloglist/')
class ImportPointLogList(APIImportBaseView):
    task = process_pointloglist
