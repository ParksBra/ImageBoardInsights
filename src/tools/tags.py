
from ..tools.iterators import get_attribute_path_values
from ..api.counts import ImageBoardCounts
from ..api.iterators import ImageBoardTagsIterator

def get_tag_post_counts(iterator:ImageBoardTagsIterator) -> ImageBoardCounts:
    counts = get_attribute_path_values(iterator, ["post_count"])
    tags = get_attribute_path_values(iterator, ["name"])
    return ImageBoardCounts(tags, counts)
