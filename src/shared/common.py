import logging
import json
from typing import Any, Iterable
import math

def load_credentials(path:str="credentials.json") -> tuple:
    logging.debug(f"Loading credentials from {path}")
    with open(path, "r") as f:
        creds = json.load(f)
    return creds["username"], creds["apikey"]

def load_preferences(path:str="preferences.json") -> dict:
    logging.debug(f"Loading preferences from {path}")
    with open(path, "r") as f:
        prefs = json.load(f)
    return prefs

def soft_match_value(search:Any, iterable:Iterable, default:Any=None) -> Any:
    for item in iterable:
        if search in item:
            return item
    return default

def soft_match_index(search:Any, iterable:Iterable) -> bool:
    for i, item in enumerate(iterable):
        if search in item:
            return i
    return -1

def soft_match_bool(search:Any, iterable:Iterable) -> bool:
    for item in iterable:
        if search in item:
            return True
    return False

def normalize(value, min_val, max_val):
    """Normalizes a value to the range 0-1."""
    if max_val - min_val == 0:
        return 0  # Avoid division by zero if all values are the same
    return (value - min_val) / (max_val - min_val)

def sigmoid(x):
    """Sigmoid activation function."""
    return 1 / (1 + math.exp(-x))

def tanh(x):
    """Tanh activation function."""
    return (math.exp(x) - math.exp(-x))/(math.exp(x) + math.exp(-x))

def dict_to_sorted_tuple_list(d:dict, sort_key:object=lambda x: x[1], sort_reverse:bool=False) -> list[tuple[str, Any]]:
    return sorted(d.items(), key=sort_key, reverse=sort_reverse)

def dict_to_sorted_dual_list(d:dict, sort_key:object=lambda x: x[1], sort_reverse:bool=False) -> tuple[list[str], list[Any]]:
    zipped = tuple(zip(*dict_to_sorted_tuple_list(d, sort_key, sort_reverse)))
    if len(zipped) == 0:
        return [], []
    return list(zipped[0]), list(zipped[1])

def get_attribute_path_value(item:dict, attribute_path:list[str]|str, default:Any=None) -> Any:
    if isinstance(attribute_path, str):
        attribute_path = [attribute_path]
    value = item
    for key in attribute_path:
        value = value.get(key, default)
    return value

def get_attribute_path_values(items:list[dict], target_attribute_path:list[str]|str, default:Any=None) -> list[Any]:
    return [get_attribute_path_value(item, target_attribute_path, default) for item in items]

def attribute_counts(iterator:Iterable, target_attribute_path:list[str]|str, default:callable=str, expand_lists:bool=True) -> dict[Any, int]:
    if isinstance(target_attribute_path, str):
        target_attribute_path = [target_attribute_path]
    value_counts = {}
    for item in iterator:
        value = get_attribute_path_value(item, target_attribute_path)
        if value == None:
            continue
        try:
            if expand_lists and isinstance(value, list):
                for v in value:
                    value_counts[v] = value_counts.get(v, 0) + 1
            else:
                value_counts[value] = value_counts.get(value, 0) + 1
        except TypeError:
            value_counts[default(value)] = value_counts.get(default(value), 0) + 1
    return value_counts
