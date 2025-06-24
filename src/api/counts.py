from __future__ import annotations
from typing import TYPE_CHECKING, Any, Generator
import statistics as stats
import logging
import pandas as pd

from ..shared.common import dict_to_sorted_dual_list, attribute_counts

if TYPE_CHECKING:
    from ..api.iterators import ImageBoardIterator

class ImageBoardCounts:
    def __init__(self, values: list[Any] = None, counts: list[int] = None):
        if values is None:
            values = list()
        if counts is None:
            counts = list()

        # Initialize DataFrame
        self._df = pd.DataFrame({
            'value': values,
            'count': counts
        })

    def __len__(self):
        return len(self.index)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return ImageBoardCounts(
                self._df['value'].iloc[index].tolist(),
                self._df['count'].iloc[index].tolist()
            )
        else:
            return ImageBoardCounts(
                [self._df['value'].iloc[index]],
                [self._df['count'].iloc[index]]
            )

    def __repr__(self):
        return self._df.__repr__()

    def __str__(self):
        return self._df.__str__()

    def __getattr__(self, name):
        # Forward any missing attributes to DataFrame
        try:
            return super().__getattr__(name)
        except AttributeError:
            return getattr(self._df, name)

    def __iter__(self):
        return self._df.itertuples(index=False)

    # def __add__(self, other: ImageBoardCounts) -> ImageBoardCounts:
    #     return self._from_df(pd.merge(self._df, other._df, on='value'))

    def __add__(self, other: ImageBoardCounts) -> ImageBoardCounts:
        # Merge DataFrames with outer join
        merged = pd.merge(
            self._df,
            other._df,
            on='value',
            how='outer',
            suffixes=('_1', '_2')
        )

        # Sum the counts and handle NaN
        merged['count'] = merged['count_1'].fillna(0) + merged['count_2'].fillna(0)

        # Keep only value and count columns
        result = merged[['value', 'count']]

        # Create new ImageBoardCounts from result
        return ImageBoardCounts(result['value'].tolist(), result['count'].tolist())

    def __radd__(self, other: ImageBoardCounts) -> ImageBoardCounts:
        return self + other

    @property
    def values(self):
        return self._df['value'].tolist()

    @property
    def counts(self):
        return self._df['count'].tolist()

    @property
    def max(self) -> int:
        return max(self.counts)

    @property
    def min(self) -> int:
        return min(self.counts)

    @property
    def mean(self) -> float:
        return stats.mean(self.counts)

    @property
    def median(self) -> float:
        return stats.median(self.counts)

    @property
    def mode(self) -> int:
        return stats.mode(self.counts)

    def sort(self, reverse:bool=False):
        return self._df.sort_values(['count', 'value'], ascending=reverse, inplace=True)

    def copy(self):
        return ImageBoardCounts(self.values, self.counts)

    def pop(self, index: int):
        value = self._df['value'].iloc[index]
        count = self._df['count'].iloc[index]
        self._df.drop(self._df.index[index], inplace=True)
        return value, count

    def find_value(self, value: Any) -> int:
        matches = self._df[self._df['value'] == value].index
        return matches[0] if len(matches) > 0 else -1

    @staticmethod
    def _from_df(df: pd.DataFrame, counts:ImageBoardCounts=None):
        self = counts if counts is not None else ImageBoardCounts()
        self._df = df
        if 'value' not in self._df.columns or 'count' not in self._df.columns:
            self._df['value'] = list()
            self._df['count'] = list()
        return self

    @property
    def _constructor(self):
        return ImageBoardCounts._from_df


class ImageBoardIteratorAttributeCounts(ImageBoardCounts):
    def __init__(self, iterator:ImageBoardIterator, target_attribute_path:list[str]|str, ascending:bool=True):
        self._iterator = iterator
        self.target_attribute_path = target_attribute_path if isinstance(target_attribute_path, list) else [target_attribute_path]
        self.target_attribute_path_string = f"/{'/'.join(target_attribute_path)}"
        self.ascending = ascending
        self._values, self._counts = self._calculate_iterator_counts(self._iterator, self.target_attribute_path, self.ascending)
        super().__init__(self._values, self._counts)

    def _calculate_iterator_counts(self, iterator:ImageBoardIterator, target_attribute_path:list[str]|str, ascending:bool=True) -> tuple[Any, int]:
        return dict_to_sorted_dual_list(attribute_counts(iterator, target_attribute_path), sort_reverse=not ascending)
