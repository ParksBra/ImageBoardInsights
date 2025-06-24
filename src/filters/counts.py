from typing import Any
from scipy.stats import scoreatpercentile
import logging
from ..api.counts import ImageBoardCounts

class ImageBoardCountFilterBase:
    def __init__(self, cut_on_failure:bool=False):
        self.cut_on_failure = cut_on_failure

    def _test_value(self, value:Any, parent_counts:ImageBoardCounts) -> bool:
        return True

    def _test_count(self, count:int, parent_counts:ImageBoardCounts) -> bool:
        return True

    def _test_index(self, index:int, parent_counts:ImageBoardCounts) -> bool:
        return True

    def _pre_processing(self, counts:ImageBoardCounts) -> ImageBoardCounts:
        counts = counts.copy()
        return counts

    def _filter(self, counts:ImageBoardCounts, cut_on_failure:bool=False) -> ImageBoardCounts:
        indexes_to_filter = []
        for i, count in enumerate(counts):
            v = count[0]
            c = count[1]
            logging.debug(f"{i}: Testing value {v} ({c})")
            pass_flag = True
            if not self._test_value(v, counts):
                logging.debug(f"{i}: Value {v} ({c}) failed value filter test")
                pass_flag = False
            if not self._test_count(c, counts):
                logging.debug(f"{i}: Value {v} ({c}) failed count filter test")
                pass_flag = False
            if not self._test_index(i, counts):
                logging.debug(f"{i}: Value {v} ({c}) failed index filter test")
                pass_flag = False
            if not pass_flag:
                logging.debug(f"{i}: Value {v} ({c}) failed filter test, removing")
                indexes_to_filter.append(i)
                if cut_on_failure:
                    return counts[:i]
        for i, index in enumerate(indexes_to_filter):
            logging.debug(f"target: {index - i}, length: {len(counts)}")
            counts.pop(index - i)
        return counts

    def __call__(self, counts:ImageBoardCounts) -> ImageBoardCounts:
        counts = self._pre_processing(counts)
        return self._filter(counts, self.cut_on_failure)

class ImageBoardCountFilterTop(ImageBoardCountFilterBase):
    def __init__(self, return_limit:int=-1):
        super().__init__(cut_on_failure=True)
        self.return_limit = return_limit
        self.limit_reached = False

    def _pre_processing(self, counts:ImageBoardCounts):
        counts = super()._pre_processing(counts)
        self.limit_reached = False
        counts.sort(reverse=False)
        return counts

    def _test_index(self, index:int, parent_counts:ImageBoardCounts) -> bool:
        if self.limit_reached:
            return False
        if self.return_limit != -1 and index >= self.return_limit:
            print(f"Limit reached at {index} with {self.return_limit}")
            self.limit_reached = True
            return False
        return True

class ImageBoardCountFilterMinCount(ImageBoardCountFilterBase):
    def __init__(self, min_count:int=None):
        super().__init__(cut_on_failure=True)
        self.min_count = min_count
        self.limit_reached = False

    def _pre_processing(self, counts:ImageBoardCounts):
        counts = super()._pre_processing(counts)
        self.limit_reached = False
        counts.sort(reverse=False)
        return counts

    def _test_count(self, count:int, parent_counts:ImageBoardCounts) -> bool:
        if self.limit_reached:
            return False
        if self.min_count == None or count < self.min_count:
            print(f"Limit reached at {count} with {self.min_count}")
            self.limit_reached = True
            return False
        return True

class ImageBoardCountFilterBottom(ImageBoardCountFilterBase):
    def __init__(self, return_limit:int=-1):
        super().__init__(cut_on_failure=True)
        self.return_limit = return_limit
        self.limit_reached = False

    def _pre_processing(self, counts:ImageBoardCounts):
        counts = super()._pre_processing(counts)
        self.limit_reached = False
        counts.sort(reverse=True)
        return counts

    def _test_index(self, index:int, parent_counts:ImageBoardCounts) -> bool:
        if self.limit_reached:
            return False
        if self.return_limit != -1 and index >= self.return_limit:
            print(f"Limit reached at {index} with {self.return_limit}")
            self.limit_reached = True
            return False
        return True


class ImageBoardCountFilterMaxCount(ImageBoardCountFilterBase):
    def __init__(self, max_count:int=None):
        super().__init__(cut_on_failure=True)
        self.max_count = max_count
        self.limit_reached = False

    def _pre_processing(self, counts:ImageBoardCounts):
        counts = super()._pre_processing(counts)
        self.limit_reached = False
        counts.sort(reverse=True)
        return counts

    def _test_count(self, count:int, parent_counts:ImageBoardCounts) -> bool:
        if self.limit_reached:
            return False
        if self.max_count == None or count > self.max_count:
            print(f"Limit reached at {count} with {self.max_count}")
            self.limit_reached = True
            return False
        return True

class ImageBoardCountFilterRange(ImageBoardCountFilterBase):
    def __init__(self, min_count:int=None, max_count:int=None):
        super().__init__(cut_on_failure=False)
        self.min_count = min_count
        self.max_count = max_count

    def __call__(self, counts):
        if self.min_count != None:
            counts = ImageBoardCountFilterMinCount(self.min_count)(counts)
        if self.max_count != None:
            counts = ImageBoardCountFilterMaxCount(self.max_count)(counts)
        return counts


class ImageBoardCountFilterPercentile(ImageBoardCountFilterBase):
    def __init__(self, percentile:float=0.1, upper_bound:bool=True):
        super().__init__(cut_on_failure=False)
        self.percentile = percentile
        self.upper_bound = upper_bound

    def __call__(self, counts:ImageBoardCounts) -> ImageBoardCounts:
        minimum_value = scoreatpercentile(counts.counts, self.percentile * 100)
        print(f"Minimum value: {minimum_value}")
        if self.upper_bound:
            return ImageBoardCountFilterMinCount(minimum_value)(counts)
        else:
            return ImageBoardCountFilterMaxCount(minimum_value)(counts)

class ImageBoardValueBlacklist(ImageBoardCountFilterBase):
    def __init__(self, blacklist:set[Any]):
        super().__init__(cut_on_failure=False)
        self.blacklist = blacklist

    def _test_value(self, value:Any, parent_counts:ImageBoardCounts) -> bool:
        return value not in self.blacklist

class ImageBoardValueWhitelist(ImageBoardCountFilterBase):
    def __init__(self, whitelist:set[Any]):
        super().__init__(cut_on_failure=False)
        self.whitelist = whitelist

    def _test_value(self, value:Any, parent_counts:ImageBoardCounts) -> bool:
        return value in self.whitelist
