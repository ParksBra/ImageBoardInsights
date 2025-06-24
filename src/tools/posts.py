from datetime import datetime, timedelta
import logging

from ..api.iterators import ImageBoardIterator
from ..api.counts import ImageBoardCounts, ImageBoardIteratorAttributeCounts
from ..tools.iterators import get_mean_age_of_time_attribute, get_mean_of_numeric_attribute, get_iterator_length, get_counts_of_attribute_values

def mean_post_age(iterator: ImageBoardIterator) -> timedelta:
    logging.debug(f"{iterator.iterator_instance_id}: Getting mean post age")
    return get_mean_age_of_time_attribute(iterator, ["created_at"])

def mean_post_score(iterator: ImageBoardIterator) -> float:
    logging.debug(f"{iterator.iterator_instance_id}: Getting mean score")
    return get_mean_of_numeric_attribute(iterator, ["score", "total"])

def mean_post_favcount(iterator: ImageBoardIterator) -> float:
    logging.debug(f"{iterator.iterator_instance_id}: Getting mean favcount")
    return get_mean_of_numeric_attribute(iterator, ["fav_count"])

def post_count(iterator: ImageBoardIterator) -> int:
    logging.debug(f"{iterator.iterator_instance_id}: Getting post count")
    return get_iterator_length(iterator)

def post_tag_counts(iterator: ImageBoardIterator, tag_types:list=None) -> dict[str, ImageBoardIteratorAttributeCounts]:
    logging.debug(f"{iterator.iterator_instance_id}: Getting tag counts")
    if tag_types is None:
        ["general", "species", "character", "artist", "invalid", "lore", "meta"]
    return {tag_type: get_counts_of_attribute_values(iterator, ["tags", tag_type]) for tag_type in tag_types}

def post_tag_counts_combined(iterator: ImageBoardIterator, tag_types:list=None) -> ImageBoardIteratorAttributeCounts:
    logging.debug(f"{iterator.iterator_instance_id}: Getting tag counts")
    if tag_types is None:
        ["general", "species", "character", "artist", "invalid", "lore", "meta"]
    return sum([get_counts_of_attribute_values(iterator, ["tags", tag_type]) for tag_type in tag_types], start=ImageBoardCounts())

