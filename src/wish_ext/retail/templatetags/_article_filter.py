from django import template
from django.template.defaultfilters import stringfilter
from django.utils.safestring import mark_safe

from ..models import ProductBase

register = template.Library()

@register.filter
@stringfilter
def article_status_display(status: str):
    display_dict = {
        ProductBase.STATE_DRAFT: '草稿',
        ProductBase.STATE_PUBLISHED: '已發布',
        ProductBase.STATE_PRIVATE: '私人',
        ProductBase.STATE_UNSET: '未知',
    }
    class_dict = {
        ProductBase.STATE_DRAFT: 'text-muted',
        ProductBase.STATE_PUBLISHED: 'text-info',
        ProductBase.STATE_PRIVATE: 'text-muted',
        ProductBase.STATE_UNSET: 'text-muted',
    }
    display_text = display_dict.get(status, '未知')
    display_class = class_dict.get(status, 'text-muted')
    return mark_safe(f'<span class="{display_class}">{display_text}</span>')
