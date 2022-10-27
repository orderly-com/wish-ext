from extension.extension import Extension

from django.db.models import Count, Min, Sum, Max, F, Func, Value, CharField, Q
from django.db.models.functions import Extract


class WishExtension(Extension):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

wish_ext = WishExtension()

wish_ext.client_info_panel_templates = [
]
