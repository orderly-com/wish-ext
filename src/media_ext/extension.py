from extension.extension import Extension

from django.db.models import Count, Min, Sum, Max, F, Func, Value, CharField, Q
from django.db.models.functions import Extract


class WishExtension(Extension):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_match_function = lambda: False
        self.read_match_policy_level = -1

wish_ext = WishExtension()

wish_ext.client_info_panel_templates = [
    'team/clients/_wish_info.html'
]
