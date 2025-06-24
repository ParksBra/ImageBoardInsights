from datetime import datetime, UTC, timedelta
import logging

from typing import Any
from ..api.iterators import ImageBoardPostsIterator
from ..api.counts import ImageBoardIteratorAttributeCounts
from ..shared.common import get_attribute_path_value, get_attribute_path_values

def get_mean_of_numeric_attribute(iterator: ImageBoardPostsIterator, target_attribute_path:list[str]|str, default:int|float=None) -> int|float|None:
    logging.debug(f"{iterator.iterator_instance_id}: Getting average numeric value of path {target_attribute_path}")
    running_sum = 0
    running_count = 0

    for i, item in enumerate(iterator):
        value = get_attribute_path_value(item, target_attribute_path, default)
        if value == None:
            logging.warning(f"{iterator.iterator_instance_id}: Item at index {i} does not have value at path {target_attribute_path}")
            continue
        running_count += 1
        running_sum += value
    if running_count == 0:
        return default
    return running_sum / running_count

def get_mean_age_of_time_attribute(iterator: ImageBoardPostsIterator, target_attribute_path:list[str]|str, default:timedelta|datetime=None) -> timedelta|None:
    logging.debug(f"{iterator.iterator_instance_id}: Getting average numeric value of path {target_attribute_path}")
    running_age_sum = timedelta()
    running_count = 0
    reference_dt = datetime.now(UTC)

    for i, item in enumerate(iterator):
        value = get_attribute_path_value(item, target_attribute_path, default)
        if value == None:
            logging.warning(f"{iterator.iterator_instance_id}: Item at index {i} does not have value at path {target_attribute_path}")
            continue
        elif isinstance(value, timedelta):
            value_dt = datetime.now(UTC) - value
        elif isinstance(value, datetime):
            value_dt = value
        else:
            value_dt = datetime.fromisoformat(value)

        running_count += 1
        running_age_sum += (reference_dt.astimezone(UTC) - value_dt.astimezone(UTC))

    if running_count == 0:
        return default
    return running_age_sum / running_count

def get_attribute_list(iterator: ImageBoardPostsIterator, target_attribute_path:list[str]|str, default:Any=None) -> list[Any]:
    logging.debug(f"{iterator.iterator_instance_id}: Getting values at path {target_attribute_path}")
    return get_attribute_path_values(iterator, target_attribute_path, default)

def get_iterator_length(iterator: ImageBoardPostsIterator) -> int:
    logging.debug(f"{iterator.iterator_instance_id}: Getting length of iterator")
    return len(iterator)

def get_counts_of_attribute_values(iterator: ImageBoardPostsIterator, target_attribute_path:list[str]|str, ascending:bool=True) -> tuple[Any, int]:
    logging.debug(f"{iterator.iterator_instance_id}: Getting counts of attribute values at path {target_attribute_path}")
    return ImageBoardIteratorAttributeCounts(iterator, target_attribute_path, ascending)
