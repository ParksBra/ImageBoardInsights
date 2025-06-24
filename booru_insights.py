import logging
from os import path
from json import dumps, loads
from datetime import datetime
from time import sleep
from uuid import uuid4
from copy import deepcopy
from hashlib import md5

from src.tools.insights import get_underappreciated_favorited_artists
from src.api.main import ImageBoardApi
from src.shared.common import load_credentials, load_preferences

from src.api.main import ImageBoardApi, constants, RequestWorker, MediaCacheWorker
from src.api.iterators import ImageBoardPostsIterator
from src.tools.posts import mean_post_age, mean_post_score, mean_post_favcount, post_count, post_tag_counts_combined
from src.filters.counts import ImageBoardCountFilterPercentile, ImageBoardValueBlacklist, ImageBoardCountFilterBase, ImageBoardCountFilterTop, ImageBoardCountFilterBottom
from src.filters.iterators import ImageBoardIteratorArtistWhitelistFilter, ImageBoardIteratorArtistBlacklistFilter, ImageBoardIteratorGeneralBlacklistFilter, ImageBoardIteratorSpeciesBlacklistFilter, ImageBoardIteratorCharacterBlacklistFilter, ImageBoardIteratorLoreBlacklistFilter, ImageBoardIteratorMetaBlacklistFilter, ImageBoardIteratorInvalidBlacklistFilter
from src.shared.common import normalize, sigmoid, tanh
from src.tools.tags import get_tag_post_counts

from nicegui import ui, run

class ImageBoardPost:
    def __init__(self, api:ImageBoardApi, post:dict):
        post = deepcopy(post)
        self.id = post["id"]
        self.description = post["description"]
        self.url = f"{api.base_url}/posts/{self.id}"
        self.created_at = datetime.fromisoformat(post["created_at"])
        self.updated_at = datetime.fromisoformat(post["updated_at"])
        self.change_sequence = post["change_seq"]
        self.file_url = post["file"]["url"]
        self.file_details = post["file"]
        self.preview_url = post["preview"]["url"]
        self.preview_details = post["preview"]
        self.sample_url = post["sample"]["url"]
        self.sample_details = post["sample"]

        self.tags_artist = post["tags"]["artist"]
        self.tags_general = post["tags"]["general"]
        self.tags_species = post["tags"]["species"]
        self.tags_character = post["tags"]["character"]
        self.tags_meta = post["tags"]["meta"]
        self.tags_invalid = post["tags"]["invalid"]
        self.tags_lore = post["tags"]["lore"]
        self.tags_locked = post["locked_tags"]
        self.tags = list(set(self.tags_artist + self.tags_general + self.tags_species + self.tags_character + self.tags_meta + self.tags_invalid + self.tags_lore + self.tags_locked))

        self.total_score = post["score"]["total"]
        self.upvotes = post["score"]["up"]
        self.downvotes = post["score"]["down"]
        self.fav_count = post["fav_count"]
        self.is_favorited = post["is_favorited"]
        self.comment_count = post["comment_count"]

        self.rating = post["rating"]
        self.sources = post["sources"]
        self.pools = post["pools"]
        self.has_children = post["relationships"]["has_children"]
        self.has_parent = post["relationships"]["parent_id"] is not None
        self.children_ids = post["relationships"]["children"] or []
        self.parent_ids = post["relationships"]["parent_id"] or []

        self.flags_pending = post["flags"]["pending"]
        self.flags_deleted = post["flags"]["deleted"]
        self.flags_deletion_pending = post["flags"]["flagged"]
        self.flags_note_locked = post["flags"]["note_locked"]
        self.flags_status_locked = post["flags"]["status_locked"]

        self.approver_id = post["approver_id"]
        self.uploader_id = post["uploader_id"]

class FavoriteButton(ui.button):
    def __init__(self, post:ImageBoardPost, api:ImageBoardApi, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.post = post
        self.api = api

        self.on_click(self._toggle_favorite_click)

    async def _toggle_favorite_click(self):
        self.disable()
        await self._toggle_favorite()
        self.enable()
        self.update()
        print("Updated")

    def update(self) -> None:
        logging.debug(f"Updating favorite button for post {self.post.id}")
        if self.post.is_favorited:
            self.set_icon("sym_s_heart_minus")
            self.props('flat fab color=white')
            self.classes('absolute top-0 left-0 m-2')
            self.tooltip("Unfavorite post")
        else:
            self.set_icon("sym_o_heart_plus")
            self.props('flat fab color=white')
            self.classes('absolute top-0 left-0 m-2 max-h-32 max-w-32 ')
            self.tooltip("Favorite post")
        super().update()

    async def _toggle_favorite(self):
        if self.post.is_favorited:
            request_id = self.api.unfavorite_post(self.post.id)
            response = await self.api.async_get_response(request_id=request_id, timeout=10)
            logging.debug(f"Unfavoriting post {self.post.id}")
            if response.status_code < 400 or response.json().get("message", "") == "You have already favorited this post":
                self.post.is_favorited = False
                logging.debug(f"Unfavorited post {self.post.id}")
                return ui.notify("Unfavorited post")
            logging.debug(f"Failed to unfavorite post {self.post.id}")
            return ui.notify("Failed to unfavorite post")

        request_id = self.api.favorite_post(self.post.id)
        response = await self.api.async_get_response(request_id=request_id, timeout=10)
        logging.debug(f"Unfavoriting post {self.post.id}")
        if response.status_code < 400:
            self.post.is_favorited = True
            logging.debug(f"Favorited post {self.post.id}")
            return ui.notify("Favorited post")
        logging.debug(f"Failed to favorite post {self.post.id}")
        return ui.notify("Failed to favorite post")

class ScoreButton(ui.element):
    def __init__(self, post:ImageBoardPost, api:ImageBoardApi, *args, **kwargs):
        super().__init__('q-fab', *args, **kwargs)

        self.post = post
        self.api = api

        with self:
            with ui.hbox():
                self.upvote_button.on_click(self._upvote_click)
                self.downvote_button.on_click(self._downvote_click)
                self.score_text

    async def _upvote(self):
        request_id = self.api.upvote_post(self.post.id)
        response = await self.api.async_get_response(request_id=request_id, timeout=10)
        if response.status_code < 400:
            self.post.upvotes += 1
            self.update()
            return ui.notify("Upvoted post")
        return ui.notify("Failed to upvote post")

    async def _downvote(self):
        request_id = self.api.downvote_post(self.post.id)
        response = await self.api.async_get_response(request_id=request_id, timeout=10)
        if response.status_code < 400:
            self.post.downvotes += 1
            self.update()
            return ui.notify("Downvoted post")
        return ui.notify("Failed to downvote post")

    async def _unvote(self):
        request_id = self.api.unvote_post(self.post.id)
        response = await self.api.async_get_response(request_id=request_id, timeout=10)
        if response.status_code < 400:
            self.post.upvotes -= 1
            self.update()
            return ui.notify("Unvoted post")
        return ui.notify("Failed to unvote post")

class ImageBoardPostElement(ui.interactive_image):
    def __init__(self, post:dict|ImageBoardPost, api:ImageBoardApi, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post = post
        if isinstance(post, dict):
            self.post = ImageBoardPost(api, post)
        self.api = api
        self.file_cache_path = self.api.media_download_worker.generate_path_from_url(self.post.file_url)
        self.is_source = False
        self.favorite_button = None

        with self:
            self._init_elements()

        if not path.exists(self.file_cache_path):
            self.set_source(self.post.file_url)
            self.api.media_download_worker.put_request(self.post.file_url)
        ui.timer(0.1, self.set_cached_source)


    def set_cached_source(self):
        if self.is_source:
            return
        if path.exists(self.file_cache_path):
            self.set_source(self.file_cache_path)
            self.is_source = True

    def _init_elements(self):
        self.favorite_button = FavoriteButton(post=self.post, api=self.api)



class MainUI:
    def __init__(self):
        self.credentials_path = "configuration/credentials.json"
        self.preferences_path = "configuration/preferences.json"
        self.loaded_credentials = load_credentials(self.credentials_path)
        self.loaded_preferences = load_preferences(self.preferences_path)
        self.username = self.loaded_credentials[0]
        self.api_key = self.loaded_credentials[1]
        self.base_url = self.loaded_preferences.get("base_url", "")
        self.blacklisted_tags = self.loaded_preferences.get("blacklisted_tags", {})
        self.base_search_tags = self.loaded_preferences.get("base_search_tags", [])
        self.base_iterator_filters = [
            ImageBoardIteratorGeneralBlacklistFilter(self.blacklisted_tags.get("general", [])),
            ImageBoardIteratorSpeciesBlacklistFilter(self.blacklisted_tags.get("species", [])),
            ImageBoardIteratorArtistBlacklistFilter(self.blacklisted_tags.get("artist", [])),
            ImageBoardIteratorCharacterBlacklistFilter(self.blacklisted_tags.get("character", [])),
            ImageBoardIteratorLoreBlacklistFilter(self.blacklisted_tags.get("lore", [])),
            ImageBoardIteratorMetaBlacklistFilter(self.blacklisted_tags.get("meta", [])),
            ImageBoardIteratorInvalidBlacklistFilter(self.blacklisted_tags.get("invalid", [])),
        ]

        self.e6api = ImageBoardApi(self.username, self.api_key, self.base_url, base_search_tags=self.base_search_tags)
        self.media_download_worker = MediaCacheWorker(
            target_path=path.join(self.e6api.cache_directory_relative_path, "media"),
            max_concurrent_threads=32,
            base_requests_per_minute_limit=500,
            burst_requests_per_minute_limit=1000,
            max_burst_length_seconds=120,
            min_burst_length_seconds=60,
            burst_cooldown_length_seconds=30,
            max_consecutive_burst_periods=4,
        )

        self.displayed_media = None

        self.highest_loaded_index = -1
        self.bottomed_out = False

    def get_last_days_of_favorite_artists(self, username, days:int=7, artist_favcount_top_percentile:float=0.1, extra_filters:list=None, sort_key:str="fav_count", sort_ascending:bool=False):
        if extra_filters is None:
            extra_filters = []
        # Get all favorite posts
        all_favorite_posts = self.e6api.list_posts([f"fav:{username}"])
        # Determine how many posts per artist are in the favorites
        artist_counts = post_tag_counts_combined(all_favorite_posts, ["artist"])
        # Filter out non-artist tags
        top_artist_counts = ImageBoardValueBlacklist(constants.NONARTIST_ARTISTS)(artist_counts)
        # Filter the top 25 percentile of artists by count of posts in favorites
        top_artist_counts = ImageBoardCountFilterPercentile(artist_favcount_top_percentile)(top_artist_counts)
        # Extract just the artist names
        top_artists = top_artist_counts.values
        # Create new base search for all posts that arent favorited by the user and are within the last X days
        new_search = [f"date:{days}_days_ago", f"-fav:{username}"]
        # Append filters for the top artists, and to blacklist certain tags
        search_filters = [
            ImageBoardIteratorArtistWhitelistFilter(top_artists),
        ]
        search_filters.extend(extra_filters)
        # Get the iterator for the new search, set for ui display
        new_posts_from_top_artists = self.e6api.list_posts(new_search, filters=search_filters, overwrite_disk_cache=False)
        # Sort the posts by favorite count to show the most popular first
        new_posts_from_top_artists.sort_values(sort_key, ascending=sort_ascending)
        return new_posts_from_top_artists

    async def set_post(self, index):
        logging.debug(f"Getting {index} ({len(self.displayed_post_iterator)})")
        try:
            self.displayed_post_iterator[index]
        except IndexError:
            logging.debug(f"Bottomed out {index} ({len(self.displayed_post_iterator)})")
            self.bottomed_out = True
            return
        post = dict(self.displayed_post_iterator[index])
        print(post.__class__)
        if self.displayed_media is not None:
            self.displayed_media.delete()
        self.displayed_media = ImageBoardPostElement(post, self.e6api)
        # media_path = self.media_download_worker.generate_path_from_url(media_url)
        # if not path.exists(media_path):
        #     self.media_download_worker.put_request(media_url)
        #     media_path = media_url
        # logging.debug(f"Loading media at index {index}: {media_url} (Source: {media_path})")

        # if self.displayed_media is not None:
        #     self.displayed_media.delete()

        # if media_path.endswith(".webm") or media_path.endswith(".mp4"):
        #     self.displayed_media = ui.video(media_path, loop=True, autoplay=True, muted=True)
        # else:
        #     self.displayed_media = ui.image(media_path)
        # with self.displayed_media:
        #     with ui.context_menu():
        #         ui.menu_item("Favorite", lambda: self.e6api.favorite_post(post_id))
        #         ui.separator()
        #         ui.menu_item(f"Index {self.highest_loaded_index}")
        #         for artist in artists:
        #             ui.menu_item(f"{self.e6api.base_url}posts?tags={artist}")
        #         ui.menu_item(f"{media_url}")


    async def scan_page(self):
        self.displayed_post_iterator = self.get_last_days_of_favorite_artists(self.username, days=7, artist_favcount_top_percentile=0.25, extra_filters=self.base_iterator_filters, sort_key="fav_count", sort_ascending=False)



        async def next_post():
            if self.bottomed_out:
                logging.debug(f"Cannot go further: {self.highest_loaded_index}")
                return
            self.highest_loaded_index += 1
            await self.set_post(self.highest_loaded_index)

        async def previous_post():
            if self.highest_loaded_index <= 0:
                logging.debug(f"Cannot go back any further: {self.highest_loaded_index}")
                return
            if self.bottomed_out:
                self.bottomed_out = False
            self.highest_loaded_index -= 1
            await self.set_post(self.highest_loaded_index)

        async def favorite_and_next():
            self.e6api.favorite_post(self.displayed_post_iterator[self.highest_loaded_index]["id"])
            await next_post()

        async def handle_keypress(event):
            if not event.action.keyup:
                return
            if event.key == "ArrowRight":
                await next_post()
            elif event.key == "ArrowLeft":
                await previous_post()
            elif event.key == "f":
                await favorite_and_next()


        ui.keyboard(on_key=handle_keypress)
        await next_post()


    async def page(self):
        async def check():
            if (not self.bottomed_out) and await ui.run_javascript('window.pageYOffset >= document.body.offsetHeight - 2 * window.innerHeight', timeout=10):
                self.highest_loaded_index += 1
                logging.debug(f"Getting {self.highest_loaded_index} ({len(self.displayed_post_iterator)})")
                try:
                    self.displayed_post_iterator[self.highest_loaded_index]
                except IndexError:
                    logging.debug(f"Bottomed out {self.highest_loaded_index} ({len(self.displayed_post_iterator)})")
                    self.bottomed_out = True
                    return


        self.displayed_post_iterator = self.get_last_days_of_favorite_artists(self.username, days=7, artist_favcount_top_percentile=0.25, extra_filters=self.base_iterator_filters, sort_key="fav_count", sort_ascending=False)
        await ui.context.client.connected()
        ui.timer(0.1, check)


@ui.page('/')
async def main():
    logging.basicConfig(level=logging.DEBUG)
    main_ui = MainUI()
    await main_ui.scan_page()

ui.run(dark=True, show=False)
