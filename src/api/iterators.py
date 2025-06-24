from __future__ import annotations
from typing import TYPE_CHECKING, Any
import logging
import json
import os
from uuid import uuid4
from hashlib import md5
import pandas as pd

from ..shared.common import dict_to_sorted_tuple_list
from ..api import constants

if TYPE_CHECKING:
    from ..api.main import ImageBoardApi
    from requests import Response

class IterablePath:
    def __init__(self, *args:list[int|str|slice], default:Any=None):
        self.path = args
        self.default = default
        self.str_delimiter = "->"

    def get(self, obj:Any):
        target = obj
        for path_element in self.path:
            logging.debug(f"Getting path element {path_element}")
            if isinstance(path_element, slice) or isinstance(path_element, int):
                try:
                    target = target[path_element]
                except IndexError:
                    target = self.default
            elif isinstance(path_element, str):
                target = target.get(path_element, self.default)
            else:
                raise ValueError(f"Invalid path element type {type(path_element)}")
        return target

    def __str__(self):
        return self.str_delimiter.join([str(element) for element in self.path])

    def __repr__(self):
        return f"IterablePath({', '.join([str(element) for element in self.path])}, default={self.default})"

    def __call__(self, obj:Any):
        return self.get(obj)

class DiskCache:
    def __init__(self, cache_file_path:str, lazy_lookahead:int=0, clear_cache_on_init:bool=False):
        self.cache_file_path = cache_file_path
        self.lazy_lookahead = lazy_lookahead
        self.cache_source_available = True
        self._iter_next_index = 0
        self._df = None
        if clear_cache_on_init:
            logging.info(f"Init cache clear for {self.cache_file_path}")
            self._clear_cache()

        logging.debug(f"Init cache load for {self.cache_file_path}")
        self._load_disk_cache()

    def _get_next_data(self) -> list[dict]:
        return []

    def _clear_cache(self) -> None:
        logging.info(f"Clearing cache for {self.cache_file_path}")
        self._update_disk_cache(pd.DataFrame())

    def _append_from_source(self) -> bool:
        if self._df is None:
            logging.debug("Dataframe is None, creating new dataframe when appending from source")
            self._df = pd.DataFrame()
        new_data = self._get_next_data()
        if len(new_data) == 0:
            self.cache_source_available = False
            return False
        self._df = pd.concat([self._df, pd.DataFrame(new_data)], ignore_index=True)
        logging.debug(f"Appended {len(new_data)} items ({len(self._df)}) from source to cache for {self.cache_file_path}")
        self._update_disk_cache()
        self.cache_source_available = True
        return True

    def _update_disk_cache(self, _df:pd.DataFrame=None) -> None:
        if _df is None:
            logging.debug("Updating cache with current dataframe")
            _df = self._df
        os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
        with open(self.cache_file_path, "wb") as f:
            _df.to_pickle(f)
        logging.debug(f"Updated cache for {self.cache_file_path} with {len(_df)} items")
        logging.debug(f"Cache file size: {os.path.getsize(self.cache_file_path)}")

    def _load_disk_cache(self) -> None:
        logging.debug(f"Loading cache for {self.cache_file_path}")
        if not self._check_disk_cache():
            self._clear_cache()
            logging.debug(f"Cache for {self.cache_file_path} does not exist, creating new cache")
            return self._load_disk_cache()
        with open(self.cache_file_path, "rb") as f:
            logging.debug(f"Loaded cache file size: {os.path.getsize(self.cache_file_path)}")
            self._df = pd.read_pickle(f)
        logging.debug(f"Loaded cache for {self.cache_file_path} with {len(self._df)} items")

    def _check_disk_cache(self) -> bool:
        status = os.path.exists(self.cache_file_path)
        logging.debug(f"Checking cache for {self.cache_file_path}: {status}")
        return status

    def __iter__(self):
        logging.info("Init iteration for PagedDiskCache")
        self._iter_next_index = 0
        return self

    def __next__(self):
        if self.cache_source_available and self._iter_next_index + self.lazy_lookahead >= len(self._df):
            logging.debug("Appending from source via next")
            self._append_from_source()
        if self._iter_next_index >= len(self._df):
            raise StopIteration
        result = self._df.iloc[self._iter_next_index]
        self._iter_next_index += 1
        return result

    def sort_values(self, by:str|list[str], ascending:bool=True, complete_cache:bool=True):
        if complete_cache:
            self.complete_cache()
        if len(self._df) == 0:
            return
        self._df.sort_values(by=by, ascending=ascending, inplace=True)
        self._update_disk_cache()

    def sort_index(self, ascending:bool=True, complete_cache:bool=True):
        if complete_cache:
            self.complete_cache()
        self._df.sort_index(ascending=ascending, inplace=True)
        self._update_disk_cache()

    def complete_cache(self):
        while self.cache_source_available:
            self._append_from_source()

    def __len__(self):
        self.complete_cache()
        return len(self._df)

    def __getitem__(self, index):
        try:
            return self._df.iloc[index]
        except IndexError:
            if self.cache_source_available:
                self._append_from_source()
                return self.__getitem__(index)
            raise IndexError

class ImageBoardIterator(DiskCache):
    def __init__(self, api:ImageBoardApi, endpoint:str, filters:list[callable]=None, lazy_lookahead:int=0, iterator_type:str="iterator", clear_cache_on_init:bool=False, starting_page:int=1, **kwargs):
        self.api = api
        self.endpoint = endpoint
        self.filters = filters if filters is not None else list()
        self.request_data = kwargs
        self.iterator_instance_id = uuid4()
        self.type = iterator_type
        self.starting_page = starting_page
        self._next_page = self.starting_page
        self.request_hash = self._hash()
        super().__init__(self._get_cache_file_path(), lazy_lookahead=lazy_lookahead, clear_cache_on_init=clear_cache_on_init)
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Created iterator for {endpoint}")

    def _filter_page_items(self, page_items:list[dict]) -> list[dict]:
        return [item for item in page_items if all([filter(item) for filter in self.filters])]

    def _handle_ImageBoard_page(self, response:Response|dict, page:int) -> list[dict]:
        page_items = response[list(response.keys())[0]]
        if len(page_items) == 0:
            return []
        self._next_page = page + 1
        handled_page_items = self._handle_ImageBoard_page_items(page_items, page)
        if len(handled_page_items) == 0:
            self._next_page = page
            return self._get_next_page()
        return handled_page_items

    def _handle_ImageBoard_page_items(self, page_items:list[dict], page:int) -> list[dict]:
        if len(page_items) == 0:
            logging.info(f"{self.type}-{self.iterator_instance_id}: Page {page} is empty")
            return []
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Page {page} has {len(page_items)} items")
        if self.filters:
            filtered_page_items = self._filter_page_items(page_items)
            if len(filtered_page_items) == 0:
                logging.info(f"{self.type}-{self.iterator_instance_id}: Page {page} is empty after filtering")
                return []
            page_items = filtered_page_items
        return page_items

    def _get_next_data(self) -> list[dict]:
        if self._next_page is None:
            self._next_page = self.starting_page
        page = self._next_page
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Requesting page {page}")
        page_data = self.request_data.copy()
        page_data["page"] = page
        response = self.api.request(self.endpoint, "GET", page_data)
        final_page_items = self._handle_ImageBoard_page(response, page)
        if len(final_page_items) > 0:
            self._next_page += 1
        return final_page_items

    def _get_cache_file_path(self):
        return os.path.join(self.api.cache_directory_relative_path, self.type, f"{self.request_hash}.pkl")

    def _update_cache_dir_path(self):
        self.request_hash = self._hash()
        self.cache_file_path = self._get_cache_file_path()

    def _hash(self):
        hash_pieces = [self.api.username, self.endpoint]
        request_data = [f"{key}:{sorted(value) if isinstance(value, list) else value}" for key, value in self.request_data.items()]
        request_data.sort()
        filter_data = [str(filt.__repr__()) for filt in self.filters]
        filter_data.sort()
        hash_pieces.extend(request_data)
        request_string = "_".join(hash_pieces)
        request_hash = md5(request_string.encode()).hexdigest()
        return request_hash

class ImageBoardIDBasedIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, endpoint:str, filters:list[callable]=None, iterator_type:str="id_iterator", clear_cache_on_init:bool=False, starting_id:int=0, reverse_responses:bool=False, id_value_path:IterablePath=None, **kwargs):
        super().__init__(api, endpoint, filters=filters, iterator_type=iterator_type, clear_cache_on_init=clear_cache_on_init, starting_page=0, **kwargs)
        self.starting_id = starting_id
        self.reverse_responses = reverse_responses
        self._next_id = self.starting_id
        self.id_value_path = id_value_path if id_value_path is not None else IterablePath("id")
        logging.debug(f"{self.type}-{self.iterator_instance_id}: ID path set to {self.id_value_path}")


    def _find_next_minimum_id(self) -> int:
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Finding next ID from current ID {self._next_id}")
        try:
            next_minimum_id = self.id_value_path(self._df.iloc[-1])
        except IndexError:
            logging.info(f"{self.type}-{self.iterator_instance_id}: No items in cache, using starting ID {self.starting_id}")
            next_minimum_id = self.starting_id
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Next minimum ID is {next_minimum_id}")
        return next_minimum_id

    def _handle_ImageBoard_page(self, response:Response|dict, target_id:int):
        page_items = response[list(response.keys())[0]]
        if self.reverse_responses:
            page_items.reverse()
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Page with IDs starting at ID {target_id} has {len(page_items)} items")
        if len(page_items) == 0:
            logging.info(f"{self.type}-{self.iterator_instance_id}: Page with IDs starting at ID {target_id} is empty")
            return []
        last_item = page_items[-1]
        last_id = self.id_value_path(last_item)
        handled_page_items = self._handle_ImageBoard_page_items(page_items, target_id)
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Handled page with IDs starting at ID {target_id} has {len(handled_page_items)} items")
        if len(handled_page_items) == 0:
            return self._get_next_data(last_id)
        return handled_page_items

    def _handle_ImageBoard_page_items(self, page_items:list[dict], target_id:int) -> list[dict]:
        if len(page_items) == 0:
            logging.info(f"{self.type}-{self.iterator_instance_id}: Page with IDs starting at ID {target_id} is empty")
            return []
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Page with IDs starting at ID {target_id} has {len(page_items)} items")
        if self.filters:
            filtered_page_items = self._filter_page_items(page_items)
            if len(filtered_page_items) == 0:
                logging.info(f"{self.type}-{self.iterator_instance_id}: Page with IDs starting at ID {target_id} is empty after filtering")
                return []
            page_items = filtered_page_items
        return page_items

    def _get_next_data(self, target_id:int=None) -> list[dict]:
        if not self.cache_source_available:
            return []
        if self._next_page is None:
            self._next_page = self.starting_page
        if len(self._df) > 0:
            self.sort_values("id", ascending=True, complete_cache=False)
        target_id = self._find_next_minimum_id() if target_id is None else target_id
        self._next_page += 1
        logging.debug(f"{self.type}-{self.iterator_instance_id}: Requesting page with IDs starting at ID {target_id} (page {self._next_page})")
        page_data = self.request_data.copy()
        page_data["page"] = f"a{target_id}"
        response = self.api.request(self.endpoint, "GET", page_data)
        final_page_items = self._handle_ImageBoard_page(response, target_id)
        return final_page_items

class ImageBoardPostsIterator(ImageBoardIDBasedIterator):
    def __init__(self, api:ImageBoardApi, tags:list=None, filters:list[callable]=None, limit:int=320, clear_cache_on_init:bool=False):
        banned_metatags = ["order", "limit", "page", "tags"]
        if tags is None:
            tags = list()
        if not isinstance(tags, list):
            tags = list(tags)

        tags_to_remove = list()
        for tag in tags:
            if ':' in tag:
                metatag = tag.split(':')[0]
                if metatag in banned_metatags:
                    tags_to_remove.add(tag)

        logging.debug(f"Removing banned metatags {list(tags_to_remove)} from tags")
        tags = [tag for tag in tags if tag not in tags_to_remove]
        assert len(tags) < 40, "Too many tags in query"
        tags.append("order:id")
        tags.sort()

        request_data = {
            "tags": tags,
            "limit": limit
        }
        request_data = {
            key: value for key, value in request_data.items() if value is not None
        }
        if "tags" in request_data:
            request_data["tags"] = " ".join(request_data["tags"])
            print(request_data["tags"])
        super().__init__(api, api.generate_endpoint(constants.POSTS), filters=filters, iterator_type="posts", reverse_responses=True, clear_cache_on_init=clear_cache_on_init, id_value_path=IterablePath("id"), **request_data)

class ImageBoardPostFlagsIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, post_id:str=None, creator_id:str=None, creator_name:str=None, limit:int=320, clear_cache_on_init:bool=False):
        request_data = {
            "post_id": post_id,
            "creator_id": creator_id,
            "creator_name": creator_name,
            "limit": limit
        }
        request_data = {
            f"search[{key}]": str(value) for key, value in request_data.items() if value is not None
        }
        super().__init__(api, api.generate_endpoint(constants.POST_FLAGS, self.post_id), iterator_type="post_flags", limit=limit, clear_cache_on_init=clear_cache_on_init, **request_data)


class ImageBoardFavoritesIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, user_id:str=None, limit:int=320, clear_cache_on_init:bool=False):
        request_data = {
            "user_id": user_id
        }
        request_data = {
            key: value for key, value in request_data.items() if value is not None
        }
        super().__init__(api, api.generate_endpoint(constants.FAVORITES), iterator_type="favorites", **self.request_data, limit=limit, clear_cache_on_init=clear_cache_on_init, **request_data)

class ImageBoardNotesIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, body_matches:str=None, post_id:str=None, post_tags_match:str=None, creator_name:str=None, creator_id:str=None, is_active:bool=None, limit:int=320, clear_cache_on_init:bool=False):
        request_data = {
            "body_matches": body_matches,
            "post_id": post_id,
            "post_tags_match": post_tags_match,
            "creator_name": creator_name,
            "creator_id": creator_id,
            "is_active": is_active
        }
        self.request_data = {
            f"search[{key}]": str(value) for key, value in request_data.items() if value is not None
        }
        super().__init__(api, api.generate_endpoint(constants.NOTES), iterator_type="notes", **self.request_data, limit=limit, clear_cache_on_init=clear_cache_on_init)

class ImageBoardTagsIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, name_matches:str=None, category:int=None, order:str=None, hide_empty:bool=None, has_wiki:bool=None, has_artist:bool=None, limit:int=320, clear_cache_on_init:bool=False):
        request_data = {
            "name_matches": name_matches,
            "category": category,
            "order": order,
            "hide_empty": hide_empty,
            "has_wiki": has_wiki,
            "has_artist": has_artist
        }
        self.request_data = {
            f"search[{key}]": str(value) for key, value in request_data.items() if value is not None
        }
        super().__init__(api, api.generate_endpoint(constants.TAGS), iterator_type="tags", **self.request_data, limit=limit, clear_cache_on_init=clear_cache_on_init)

class ImageBoardTagAliasesIterator(ImageBoardIterator):
    def __init__(self, api:ImageBoardApi, name_matches:str=None, antecedent_name:str=None, consequent_name:str=None, antecedent_tag_category:str=None, consequent_tag_category:str=None, creator_name:str=None, approver_name:str=None, status:str=None, order:str=None, limit:int=320, clear_cache_on_init:bool=False):
        request_data = {
            "name_matches": name_matches,
            "antecedent_name": antecedent_name,
            "consequent_name": consequent_name,
            "antecedent_tag_category": antecedent_tag_category,
            "consequent_tag_category": consequent_tag_category,
            "creator_name": creator_name,
            "approver_name": approver_name,
            "status": status,
            "order": order
        }
        self.request_data = {
            f"search[{key}]": str(value) for key, value in request_data.items() if value is not None
        }
        super().__init__(api, api.generate_endpoint(constants.TAG_ALIASES), iterator_type="tag_aliases", **self.request_data, limit=limit, clear_cache_on_init=clear_cache_on_init)
