from typing import TYPE_CHECKING, Any
from datetime import datetime, timedelta
import logging

from ..api.iterators import ImageBoardIterator, IterablePath

class ImageBoardIteratorFilterBase:
    def __init__(self):
        self.filter_args = {}
        pass

    def _pre_processing(self, iteration:dict) -> dict:
        iteration = iteration.copy()
        return iteration

    def _filter(self, iteration:dict) -> bool:
        return True

    def __call__(self, iteration:dict) -> bool:
        iteration = self._pre_processing(iteration)
        return self._filter(iteration)

    def __repr__(self) -> str:
        arg_strings = [f"{key}:{value}" for key, value in self.filter_args.items()]
        arg_strings.sort()
        arg_strings_joined = ", ".join(arg_strings)
        return f"{self.__class__.__name__}({arg_strings_joined})"


class ImageBoardIteratorListFilter(ImageBoardIteratorFilterBase):
    def __init__(self, filtered:list[list[str]|str], target_list_path:IterablePath=None, whitelist:bool=True):
        self.filtered = filtered
        self.target_list_path = target_list_path if target_list_path is not None else IterablePath()
        self.whitelist = whitelist
        self.filter_args = {
            "filtered": self.filtered,
            "target_list_path": self.target_list_path,
            "whitelist": self.whitelist
        }

    def _filter(self, iteration:dict) -> bool:
        target_list = self.target_list_path(iteration)
        for filter_values in self.filtered:
            if not isinstance(filter_values, list):
                filter_values = [filter_values]
            if all([filter_value in target_list for filter_value in filter_values]):
                logging.debug(f"Filtering {target_list} based on {self.filtered} -> {filter_values} (whitelist={self.whitelist})")
                return self.whitelist
        logging.debug(f"Did not filter {target_list} based on {self.filtered}")
        return not self.whitelist

class ImageBoardIteratorWhitelistFilter(ImageBoardIteratorListFilter):
    def __init__(self, whitelist_filtered:list[list[str]|str], target_list_path:IterablePath=None):
        super().__init__(whitelist_filtered, target_list_path=target_list_path, whitelist=True)

class ImageBoardIteratorBlacklistFilter(ImageBoardIteratorListFilter):
    def __init__(self, blacklist_filtered:list[list[str]|str], target_list_path:IterablePath=None):
        super().__init__(blacklist_filtered, target_list_path=target_list_path, whitelist=False)

class ImageBoardIteratorArtistWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "artist"))

class ImageBoardIteratorArtistBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "artist"))

class ImageBoardIteratorCharacterWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "character"))

class ImageBoardIteratorCharacterBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "character"))

class ImageBoardIteratorGeneralWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "general"))

class ImageBoardIteratorGeneralBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "general"))

class ImageBoardIteratorSpeciesWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "species"))

class ImageBoardIteratorSpeciesBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "species"))

class ImageBoardIteratorLoreWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "lore"))

class ImageBoardIteratorLoreBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "lore"))

class ImageBoardIteratorMetaWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "meta"))

class ImageBoardIteratorMetaBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "meta"))

class ImageBoardIteratorInvalidWhitelistFilter(ImageBoardIteratorWhitelistFilter):
    def __init__(self, whitelist:list):
        super().__init__(whitelist, target_list_path=IterablePath("tags", "invalid"))

class ImageBoardIteratorInvalidBlacklistFilter(ImageBoardIteratorBlacklistFilter):
    def __init__(self, blacklist:list):
        super().__init__(blacklist, target_list_path=IterablePath("tags", "invalid"))

class ImageBoardIteratorNumericRangeFilter(ImageBoardIteratorFilterBase):
    def __init__(self, minimum:int|float=-1, maximum:int|float=-1, target_number_path:IterablePath=None):
        self.minimum = minimum
        self.maximum = maximum
        self.target_number_patch = target_number_path if target_number_path is not None else IterablePath()

    def _filter(self, iteration:dict) -> bool:
        target_number = self.target_number_path(iteration)
        if self.minimum != -1 and target_number < self.minimum:
            return False
        if self.maximum != -1 and target_number > self.maximum:
            return False
        return True

class ImageBoardIteratorValueEqualsFilter(ImageBoardIteratorFilterBase):
    def __init__(self, value:Any, target_value_path:IterablePath=None):
        self.value = value
        self.target_value_path = target_value_path if target_value_path is not None else IterablePath()

    def _filter(self, iteration:dict) -> bool:
        target_value = self.target_value_path(iteration)
        return target_value == self.value

class ImageBoardIteratorFavcountRangeFilter(ImageBoardIteratorNumericRangeFilter):
    def __init__(self, minimum:int=-1, maximum:int=-1):
        super().__init__(minimum, maximum, target_number_path=IterablePath("fav_count"))

class ImageBoardIteratorScoreRangeFilter(ImageBoardIteratorNumericRangeFilter):
    def __init__(self, minimum:int=-1, maximum:int=-1):
        super().__init__(minimum, maximum, target_number_path=IterablePath("score", "total"))

class ImageBoardIteratorUpvoteCountRangeFilter(ImageBoardIteratorNumericRangeFilter):
    def __init__(self, minimum:int=-1, maximum:int=-1):
        super().__init__(minimum, maximum, target_number_path=IterablePath("score", "up"))

class ImageBoardIteratorDownvoteCountRangeFilter(ImageBoardIteratorNumericRangeFilter):
    def __init__(self, minimum:int=-1, maximum:int=-1):
        super().__init__(minimum, maximum, target_number_path=IterablePath("score", "down"))

class ImageBoardIteratorDatetimeRangeFilter(ImageBoardIteratorFilterBase):
    def __init__(self, minimum:datetime=None, maximum:datetime=None):
        self.minimum = minimum
        self.maximum = maximum
        super().__init__(minimum, maximum, target_datetime_path=IterablePath("created_at"))

    def _filter(self, iteration:dict) -> bool:
        target_datetime = self.target_datetime_path(iteration)
        if isinstance(target_datetime, str):
            target_datetime = datetime.fromisoformat(target_datetime)
        if self.minimum != None and target_datetime < self.minimum:
            return False
        if self.maximum != None and target_datetime > self.maximum:
            return False
        return True

class ImageBoardIteratorDatetimeSpanFilter(ImageBoardIteratorDatetimeRangeFilter):
    def __init__(self, span:timedelta=None, from_datetime:datetime=None):
        if from_datetime is None:
            from_datetime = datetime.now()
        if span is None:
            span = timedelta(seconds=0)
        self.minimum = from_datetime - span
        self.maximum = from_datetime
        super().__init__(self.minimum, self.maximum)

