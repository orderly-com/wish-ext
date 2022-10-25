import json

def format_dict(dict_string):
    try:
        return json.loads(dict_string)
    except:
        return {}

def format_price(price_string):
    try:
        return float(price_string)
    except:
        return 0

def format_bool(bool_string):
    try:
        if bool_string.lower() in ['false', 'no', '0']:
            return False
        return bool(bool_string)
    except:
        return False
