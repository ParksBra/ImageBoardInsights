import json
import logging
from base64 import b64encode
from datetime import datetime
from hashlib import md5
from queue import Queue
from threading import Thread, Event, current_thread
from time import sleep
from typing import Any, TYPE_CHECKING
from uuid import uuid4
from os import path, makedirs
import asyncio

import requests

from ..api.iterators import (
    ImageBoardFavoritesIterator, ImageBoardNotesIterator,
    ImageBoardPostFlagsIterator, ImageBoardPostsIterator,
    ImageBoardTagAliasesIterator, ImageBoardTagsIterator
)
from ..api import constants

class ManagedThread(Thread):
    def __init__(self, thread_manager:object, job_id:str, **kwargs):
        super().__init__(**kwargs)
        self.thread_manager = thread_manager
        self.job_id = job_id
        self._started_callback = self.thread_manager._thread_started_callback
        self._completed_callback = self.thread_manager._thread_completed_callback
        logging.debug(f"Managed thread {self} created")

    def run(self):
        logging.debug(f"Managed thread {self} {self.job_id} started")
        self._started_callback(self.job_id)
        super().run()
        logging.debug(f"Managed thread {self} {self.job_id} completed")
        self._completed_callback(self.job_id)

class ThreadManager:
    def __init__(self, job_queue:Queue|None=None, result_tray:dict|None=None, thread_creation_cooldown_seconds:int=0, max_concurrent_threads:int=4, new_thread_job_threshold:int=60, thread_constructor:ManagedThread=ManagedThread, **thread_kwargs):
        super().__init__()
        self.job_queue = job_queue if job_queue is not None else Queue()
        self.result_tray = result_tray if result_tray is not None else dict()
        self.thread_creation_cooldown_seconds = thread_creation_cooldown_seconds
        self.max_concurrent_threads = max_concurrent_threads
        self.new_thread_job_threshold = new_thread_job_threshold if new_thread_job_threshold > 0 else 1
        self.thread_constructor = thread_constructor
        self.thread_kwargs = thread_kwargs
        self.manager_thread = None
        self._active_threads = []

    def _thread_started_callback(self, job_id:str, **kwargs):
        if current_thread() not in self._active_threads:
            self._active_threads.append(current_thread())
        else:
            logging.error(f"Request {job_id} worker thread {current_thread()} already in active threads")

    def _thread_completed_callback(self, job_id:str, **kwargs):
        logging.debug(f"Request {job_id} worker thread {current_thread()} completed")
        try:
            self._active_threads.remove(current_thread())
        except ValueError:
            logging.error(f"Request {job_id} worker thread {current_thread()} not found in active threads")
        self.job_queue.task_done()

    def _cleanup_threads(self):
        for thread in self._active_threads:
            if thread.is_alive():
                continue
            self._active_threads.remove(thread)

    def _construct_thread(self, job_id:str, **kwargs) -> object:
        return self.thread_constructor(self, job_id, **kwargs)

    def _soft_start(self):
        if not self.manager_thread.is_alive():
            try:
                self.start()
            except RuntimeError:
                logging.error("Thread manager thread already started")

    def _summon_thread_from_queue(self, pre_summon_delay:int=0, post_summon_delay:int=0) -> object:
        job_id, job_kwargs = self.job_queue.get()
        return self._summon_thread(job_id, pre_summon_delay=pre_summon_delay, post_summon_delay=post_summon_delay, **job_kwargs)

    def _summon_thread(self, job_id:str, pre_summon_delay:int=0, post_summon_delay:int=0, **kwargs) -> object:
        logging.debug(f"Summoning thread for job {job_id}")
        sleep(pre_summon_delay)
        run_time = datetime.now().timestamp()
        thread = self._construct_thread(job_id, **kwargs)
        thread.start()
        finish_time = datetime.now().timestamp()
        run_time_delta = finish_time - run_time
        time_till_next_thread = post_summon_delay - run_time_delta
        if time_till_next_thread > 0:
            sleep(time_till_next_thread)
        return thread

    def _manage_threads_work(self):
        current_queue_size = self.job_queue.qsize()
        desired_worker_count = (current_queue_size // self.new_thread_job_threshold) + 1
        desired_worker_count = desired_worker_count if desired_worker_count <= self.max_concurrent_threads else self.max_concurrent_threads
        desired_new_worker_count = desired_worker_count - len(self._active_threads)

        if desired_new_worker_count > 0:
            logging.debug(f"Summoning new threads ({desired_new_worker_count} desired, {len(self._active_threads)} active, {self.max_concurrent_threads} max)")
            self._summon_thread_from_queue(post_summon_delay=self.thread_creation_cooldown_seconds)
        else:
            self._cleanup_threads()
            sleep_time = self.thread_creation_cooldown_seconds/10
            sleep(sleep_time if sleep_time > 0 else 0.1)

    def _manage_threads_job(self):
        logging.debug("Thread manager started")
        while self.manager_thread.is_alive() and (not self.job_queue.empty() or self._active_threads):
            self._manage_threads_work()
        logging.debug("Request worker thread finished")

    def manage_threads(self):
        if self.manager_thread is None or not self.manager_thread.is_alive():
            self.manager_thread = Thread(target=self._manage_threads_job)
            self.manager_thread.start()

    def put_job(self, **kwargs) -> str:
        job_id = uuid4()
        self.job_queue.put((job_id, kwargs))
        self.manage_threads()
        return job_id

    def get_result(self, job_id:str, timeout:int=-1) -> Any:
        timeout_time = datetime.now().timestamp() + timeout
        start_attempts = 0
        while job_id not in self.result_tray:
            sleep(0.1)
            if not self.manager_thread.is_alive():
                self.manage_threads()
                start_attempts += 1
            if start_attempts > 5:
                logging.error(f"Job ({job_id}) not found in result tray")
                raise KeyError(f"Job ({job_id}) not found in result tray")
            if timeout >= 0 and datetime.now().timestamp() > timeout_time:
                raise TimeoutError(f"Job ({job_id}) timed out after {timeout} seconds")
        return self.result_tray.pop(job_id)

    async def async_get_result(self, job_id:str, timeout:int=-1) -> Any:
        timeout_time = datetime.now().timestamp() + timeout
        start_attempts = 0
        while job_id not in self.result_tray:
            await asyncio.sleep(0.1)
            if not self.manager_thread.is_alive():
                self.manage_threads()
                start_attempts += 1
            if start_attempts > 5 and timeout < 0:
                logging.error(f"Job ({job_id}) not found in result tray")
                raise KeyError(f"Job ({job_id}) not found in result tray")
            if timeout >= 0 and datetime.now().timestamp() > timeout_time:
                raise TimeoutError(f"Job ({job_id}) timed out after {timeout} seconds")
        return self.result_tray.pop(job_id)

    def join(self):
        self.manage_threads()
        for thread in self._active_threads:
            thread.join()
        self.manager_thread.join()

class RequestThread(ManagedThread):
    def __init__(self, thread_manager:object, job_id:str, endpoint:str, method:str, headers:dict, data:dict):
        super().__init__(thread_manager, job_id, target=self._run)
        self._result_tray = self.thread_manager.result_tray
        self.endpoint = endpoint
        self.method = method
        self.headers = headers
        self.data = data
        logging.debug(f"Request thread {self} created")

    def _run(self):
        logging.debug(f"Request ({self.job_id}) to {self.endpoint} started")
        response = requests.request(self.method, self.endpoint, headers=self.headers, data=self.data)
        self._result_tray[self.job_id] = response
        if response.status_code >= 400:
            logging.error(f"Request ({self.job_id}) to {self.endpoint} returned status code {response.status_code}")
            logging.error(f"Response: {response.text}")
        else:
            logging.debug(f"Request ({self.job_id}) to {self.endpoint} returned status code {response.status_code}")

class RequestWorker(ThreadManager):
    def __init__(
            self,
            request_queue:Queue|None=None,
            response_tray:dict|None=None,
            max_concurrent_threads:int=4,
            base_requests_per_minute_limit:int=60,
            burst_requests_per_minute_limit:int=120,
            max_burst_length_seconds:int=60,
            min_burst_length_seconds:int=30,
            burst_cooldown_length_seconds:int=120,
            max_consecutive_burst_periods:int=1,
            min_resource_query_interval_in_seconds:float=0.1
        ):
        super().__init__(request_queue, response_tray, max_concurrent_threads=max_concurrent_threads, new_thread_job_threshold=1, thread_constructor=RequestThread)

        self.max_concurrent_threads = max_concurrent_threads

        self.base_requests_per_minute_limit = base_requests_per_minute_limit
        self.base_requests_per_second_limit = 60 / self.base_requests_per_minute_limit
        self.min_base_request_interval_in_seconds = 60 / self.base_requests_per_minute_limit

        self.burst_requests_per_minute_limit = burst_requests_per_minute_limit
        self.burst_requests_per_second_limit = 60 / self.burst_requests_per_minute_limit
        self.min_burst_request_interval_in_seconds = 60 / self.burst_requests_per_minute_limit

        self.max_burst_length_seconds = max_burst_length_seconds
        self.max_consecutive_burst_requests = self.burst_requests_per_second_limit * self.max_burst_length_seconds
        self.min_burst_length_seconds = min_burst_length_seconds
        self.min_consecutive_burst_requests = self.burst_requests_per_second_limit * self.min_burst_length_seconds

        self.burst_cooldown_length_seconds = burst_cooldown_length_seconds

        self.max_consecutive_burst_periods = max_consecutive_burst_periods

        self.min_resource_query_interval_in_seconds = min_resource_query_interval_in_seconds

        self._consecutive_burst_requests = 0
        self._consecutive_burst_periods = 0
        self._burst_available_after_timestamp = 0

    def _manage_threads_work(self):
        super()._manage_threads_work()
        if self._consecutive_burst_periods >= self.min_consecutive_burst_requests:
            query_run_time = datetime.now().timestamp()
            self._consecutive_burst_periods  = 0
            self._consecutive_burst_requests = 0
            self._burst_available_after_timestamp = query_run_time + self.burst_cooldown_length_seconds

    def _determine_interval(self) -> float:
        query_run_time = datetime.now().timestamp()
        cooldown_active = query_run_time <= self._burst_available_after_timestamp
        new_burst_periods_available = self._consecutive_burst_periods < self.max_consecutive_burst_periods
        new_burst_requests_available = self._consecutive_burst_requests < self.max_consecutive_burst_requests
        if not new_burst_requests_available and new_burst_periods_available:
            self._consecutive_burst_periods += 1
            self._consecutive_burst_requests = 0
            self._burst_available_after_timestamp = query_run_time + self.burst_cooldown_length_seconds
            return self._determine_interval()

        if not cooldown_active and new_burst_requests_available:
            self._consecutive_burst_requests += 1
            return self.min_burst_request_interval_in_seconds

        return self.min_base_request_interval_in_seconds

    def _summon_thread(self, job_id, **kwargs):
        kwargs.pop("post_summon_delay", None)
        return super()._summon_thread(job_id, post_summon_delay=self._determine_interval(), **kwargs)

    def put_request(self, endpoint:str, method:str="GET", headers:dict=None, data:dict=None) -> str:
        return super().put_job(endpoint=endpoint, method=method, headers=headers, data=data)

    def get_response(self, request_id:str, timeout:int=-1) -> requests.Response:
        return super().get_result(request_id, timeout)

    async def async_get_response(self, request_id:str, timeout:int=-1) -> requests.Response:
        return await super().async_get_result(request_id, timeout)

class MediaCacheWorker(RequestWorker):
    def __init__(self, target_path:str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_path = target_path
        self.media_cache_request_status_tray = dict()

    def put_request(self, url:str) -> str:
        super().put_request(url)

    def get_response(self, request_id, timeout = -1):
        return super().get_response(request_id, timeout)

    def generate_path_from_url(self, url:str):
        object_file_extension = url.split(".")[-1]
        url_md5 = md5(url.encode()).hexdigest()
        return path.join(self.target_path, f"{url_md5}.{object_file_extension}")

    def query_cache(self, url:str) -> bool:
        return path.exists(self.generate_path_from_url(url))

    def _thread_completed_callback(self, job_id:str, **kwargs):
        response = super().get_response(job_id)
        if response.status_code >= 400:
            logging.debug(f"Cache request {job_id} for {response.request.url} returned status code {response.status_code}")
            return super()._thread_completed_callback(job_id, **kwargs)
        url = response.request.url
        object_file_path = self.generate_path_from_url(url)
        makedirs(path.dirname(object_file_path), exist_ok=True)
        logging.debug(f"Cache write for {url} to {object_file_path} started")
        with open(object_file_path, "wb") as f:
            f.write(response.content)
        super()._thread_completed_callback(job_id, **kwargs)



class BaseImageBoardApi(RequestWorker):
    def __init__(
            self,
            username:str,
            api_key:str,
            base_url:str="https://ImageBoard.net",
            requests_per_minute_limit:int=120,
            custom_user_agent:str=None
        ):
        super().__init__(
            max_concurrent_threads=4,
            base_requests_per_minute_limit=60,
            burst_requests_per_minute_limit=120,
            max_burst_length_seconds=60,
            min_burst_length_seconds=30,
            burst_cooldown_length_seconds=60,
            max_consecutive_burst_periods=1,
            min_resource_query_interval_in_seconds=0.1
        )
        self.username = username
        self.base_url = base_url.rstrip("/") if "://" in base_url else f"https://{base_url}".rstrip("/")
        self.requests_per_minute_limit = requests_per_minute_limit
        self.minimum_request_interval_in_seconds = 60 / requests_per_minute_limit
        self.auth_header = self._generate_auth_header(self.username, api_key)
        self.user_agent = custom_user_agent if custom_user_agent is not None else f"ImageBoard_Insights/1.0 (by {self.username} on ImageBoard)"
        self.headers = self._generate_header_dict(self.auth_header, self.user_agent)

    def _encode_auth(self, auth:str) -> str:
        auth_bytes = auth.encode("ascii")
        auth_base64_bytes = b64encode(auth_bytes)
        return auth_base64_bytes.decode("ascii")

    def _generate_auth(self, username:str, api_key:str) -> str:
        auth = f"{username}:{api_key}"
        return self._encode_auth(auth)

    def _generate_auth_header(self, username:str, api_key:str) -> str:
        auth = self._generate_auth(username, api_key)
        logging.debug(f"Generated auth header for {username}")
        return f"Basic {auth}"

    def _generate_header_dict(self, auth:str, user_agent:str) -> dict:
        logging.debug(f"Generating headers with auth and user agent: {user_agent}")
        return {
            "User-Agent": user_agent,
            "Authorization": auth
        }

    def put_request(self, endpoint:str, method:str, data:dict|None=None):
        return super().put_request(endpoint, method, self.headers, data)

    def request(self, endpoint:str, method:str, data:dict=None, return_json:bool=True, timeout:int=-1) -> requests.Response|dict:
        request_id = self.put_request(endpoint, method, data)
        response = self.get_response(request_id, timeout=timeout)
        if return_json:
            return response.json()
        return response


class ImageBoardApi(BaseImageBoardApi):
    def __init__(
            self,
            username:str,
            api_key:str,
            base_url:str,
            base_search_tags:list=None,
            requests_per_minute_limit:int=115,
            cache_directory_relative_path:str="search_cache",
            default_request_page_size:int=320
        ):
        assert requests_per_minute_limit <= constants.MAX_REQUESTS_PER_MINUTE, f"Requests per minute limit cannot exceed {constants.MAX_REQUESTS_PER_MINUTE} ({requests_per_minute_limit})"
        super().__init__(username, api_key, base_url, requests_per_minute_limit)

        self.base_search_tags = list(set(base_search_tags)) if base_search_tags is not None else []
        self.default_request_page_size = default_request_page_size

        self.cache_directory_relative_path = cache_directory_relative_path
        self.cache_directory_file_extension = "pkl"
        self.cache_directory_path = path.join(path.dirname(__file__), self.cache_directory_relative_path)

        self.media_download_worker = MediaCacheWorker(
            target_path=path.join(self.cache_directory_relative_path, "media"),
            max_concurrent_threads=32,
            base_requests_per_minute_limit=500,
            burst_requests_per_minute_limit=1000,
            max_burst_length_seconds=120,
            min_burst_length_seconds=60,
            burst_cooldown_length_seconds=30,
            max_consecutive_burst_periods=4,
        )

    def generate_endpoint(self, category:str, *args) -> str:
        match category:
            case constants.POSTS:
                base_endpoint = constants.POSTS
            case constants.FAVORITES:
                base_endpoint = constants.FAVORITES
            case constants.POST_FLAGS:
                base_endpoint = constants.POST_FLAGS
            case constants.NOTES:
                base_endpoint = constants.NOTES
            case constants.TAGS:
                base_endpoint = constants.TAGS
            case constants.TAG_ALIASES:
                base_endpoint = constants.TAG_ALIASES
            case _:
                raise ValueError(f"Invalid category '{category}'")
        endpoint_pieces = [self.base_url, base_endpoint]
        if args:
            endpoint_pieces.extend(args)
        endpoint_pieces = [str(piece) for piece in endpoint_pieces]
        return "/".join(endpoint_pieces) + constants.URL_SUFFIX

    def favorite_post(self, post_id:str) -> str:
        return self.put_request(self.generate_endpoint(constants.FAVORITES), method="POST", data={"post_id": post_id})

    def unfavorite_post(self, post_id:str) -> str:
        return self.put_request(self.generate_endpoint(constants.FAVORITES, post_id), method="DELETE")

    def upvote_post(self, post_id:str) -> str:
        return self.put_request(self.generate_endpoint(constants.POST_FLAGS), method="POST", data={"post_id": post_id, "score": 1, "no_unvote": True})

    def downvote_post(self, post_id:str) -> str:
        return self.put_request(self.generate_endpoint(constants.POST_FLAGS), method="POST", data={"post_id": post_id, "score": -1, "no_unvote": True})

    def unvote_post(self, post_id:str) -> str:
        return self.put_request(self.generate_endpoint(constants.POST_FLAGS), method="POST", data={"post_id": post_id, "score": 0, "no_unvote": True})

    def list_posts(self, tags:list, filters:list[callable]=None, include_base_tags:bool=True, overwrite_disk_cache:bool=False):
        assert len(tags) + len(self.base_search_tags) <= constants.MAX_TAGS_PER_REQUEST, f"Maximum tags per request is {constants.MAX_TAGS_PER_REQUEST} ({len(tags) + len(self.base_search_tags)})"
        filters = filters if filters is not None else []
        combined_tags = []
        if include_base_tags:
            combined_tags = self.base_search_tags.copy()

        if not isinstance(tags, list):
            tags = list(tags)
        combined_tags.extend(tags)
        combined_tags = list(set(combined_tags))
        logging.debug(f"Combined tags: {combined_tags}")
        return ImageBoardPostsIterator(self, tags=combined_tags, filters=filters, limit=self.default_request_page_size, clear_cache_on_init=overwrite_disk_cache)

    def list_post_flags(self, post_id:str=None, creator_id:str=None, creator_name:str=None):
        return ImageBoardPostFlagsIterator(self, post_id=post_id, creator_id=creator_id, creator_name=creator_name, limit=self.default_request_page_size)

    def list_favorites(self, username:str=None):
        return ImageBoardFavoritesIterator(self, username=username, limit=self.default_request_page_size)

    def list_notes(self, body_matches:str=None, post_id:str=None, post_tags_match:str=None, creator_name:str=None, creator_id:str=None, is_active:bool=None):
        return ImageBoardNotesIterator(self, body_matches=body_matches, post_id=post_id, post_tags_match=post_tags_match, creator_name=creator_name, creator_id=creator_id, is_active=is_active, limit=self.default_request_page_size)

    def list_tags(self, name_matches:str=None, category:int=None, order:str=None, hide_empty:bool=None, has_wiki:bool=None, has_artist:bool=None):
        return ImageBoardTagsIterator(self, name_matches=name_matches, category=category, order=order, hide_empty=hide_empty, has_wiki=has_wiki, has_artist=has_artist, limit=self.default_request_page_size)

    def list_tag_aliases(self, antecedent_name:str=None, consequent_name:str=None, status:str=None, order:str=None):
        return ImageBoardTagAliasesIterator(self, antecedent_name=antecedent_name, consequent_name=consequent_name, status=status, order=order, limit=self.default_request_page_size)
