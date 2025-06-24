import logging

from ..api.main import ImageBoardApi, constants
from ..api.iterators import ImageBoardPostsIterator
from ..tools.posts import mean_post_age, mean_post_score, mean_post_favcount, post_count, post_tag_counts_combined
from ..filters.counts import ImageBoardCountFilterPercentile, ImageBoardValueBlacklist, ImageBoardCountFilterBase, ImageBoardCountFilterTop
from ..shared.common import normalize, sigmoid, tanh
from ..tools.tags import get_tag_post_counts

def calculate_relevancy_score(artist_data, min_max_values):
    """
    Calculates the relevancy score for an artist.

    Args:
        artist_data: A dictionary containing the artist's metrics.
        min_max_values: A dictionary containing the min and max values for each metric across all artists.

    Returns:
        The relevancy score (float).
    """

    # 1. Normalize metrics
    normalized_data = {}
    for metric, value in artist_data.items():
        if metric in min_max_values:
            normalized_data[metric] = normalize(
                value, min_max_values[metric]["min"], min_max_values[metric]["max"]
            )
        else:
            normalized_data[metric] = value  # Assume already normalized (e.g., unfavorited_post_percentage)

    # 2. Define weights (tune these based on your priorities)
    weights = {
        "matched_desired_tags_all_count": 0.4,
        "matched_desired_tags_top_count": 0.5,
        "matched_avoided_tags_all_count": 0.4,
        "matched_avoided_tags_top_count": 0.5,
        "mean_posts_score": 0.3,
        "mean_posts_favcount": 0.3,
        "unfavorited_post_percentage": 0.3,
        "mean_post_age_seconds": 0.1,
        "post_count": 0.2
    }

    # 3. Calculate weighted sum with activation functions
    score = (
        weights["matched_desired_tags_all_count"] * normalized_data["matched_desired_tags_all_count"]
        + weights["matched_desired_tags_top_count"] * normalized_data["matched_desired_tags_top_count"]
        - weights["matched_avoided_tags_all_count"] * normalized_data["matched_avoided_tags_all_count"]
        - weights["matched_avoided_tags_top_count"] * normalized_data["matched_avoided_tags_top_count"]
        + weights["mean_posts_score"] * sigmoid(normalized_data["mean_posts_score"])
        + weights["mean_posts_favcount"] * sigmoid(normalized_data["mean_posts_favcount"])
        + weights["unfavorited_post_percentage"] * normalized_data["unfavorited_post_percentage"]
        + weights["mean_post_age_seconds"] * normalized_data["mean_post_age_seconds"] # Already inverted in normalization step
        + weights["post_count"] * tanh(normalized_data["post_count"])
    )
    return score

def get_metrics_for_search(posts:ImageBoardPostsIterator, desired_tags_include:list=None, avoided_tags_include:list=None) -> dict:
    artist_post_count = post_count(posts)
    mean_posts_score = mean_post_score(posts)
    mean_posts_favcount = mean_post_favcount(posts)
    mean_post_age_seconds = mean_post_age(posts)
    all_tags = post_tag_counts_combined(posts, ["general", "lore", "species", "character"])
    all_tags.sort()
    percentile_filter = ImageBoardCountFilterPercentile(.75)
    top_tags = percentile_filter(all_tags)
    matched_desired_tags_all = [(tag, count) for tag, count in all_tags if tag in desired_tags_include]
    matched_desired_tags_all_count = len(matched_desired_tags_all)
    matched_desired_tags_top = [(tag, count) for tag, count in top_tags if tag in desired_tags_include]
    matched_desired_tags_top_count = len(matched_desired_tags_top)
    matched_avoided_tags_all = [(tag, count) for tag, count in all_tags if tag in avoided_tags_include]
    matched_avoided_tags_all_count = len(matched_avoided_tags_all)
    matched_avoided_tags_top = [(tag, count) for tag, count in top_tags if tag in avoided_tags_include]
    matched_avoided_tags_top_count = len(matched_avoided_tags_top)

    considered_metrics = {
        "post_count": artist_post_count,
        "mean_posts_score": mean_posts_score,
        "mean_posts_favcount": mean_posts_favcount,
        "mean_post_age_seconds": mean_post_age_seconds,
        "top_tags_count": len(top_tags),
        "top_tags": top_tags,
        "all_tags_count": len(all_tags),
        "all_tags": all_tags,
        "matched_desired_tags_all_count": matched_desired_tags_all_count,
        "matched_desired_tags_all": matched_desired_tags_all,
        "matched_desired_tags_top_count": matched_desired_tags_top_count,
        "matched_desired_tags_top": matched_desired_tags_top,
        "desired_tags_count": len(desired_tags_include),
        "desired_tags": desired_tags_include,
        "matched_avoided_tags_all_count": matched_avoided_tags_all_count,
        "matched_avoided_tags_all": matched_avoided_tags_all,
        "matched_avoided_tags_top_count": matched_avoided_tags_top_count,
        "matched_avoided_tags_top": matched_avoided_tags_top,
        "avoided_tags_count": len(avoided_tags_include),
        "avoided_tags": avoided_tags_include,
    }

    return considered_metrics

def get_metrics_for_artist(api, artist, max_post_age_days=-1, favorite_count:int=0, desired_tags_include:list=None, avoided_tags_include:list=None, include_favorites_in_search:bool=True) -> dict:
    artist_search_tags = [artist, "order:score"]
    if max_post_age_days != -1:
        artist_search_tags.append(f"date:{max_post_age_days}_days_ago")
        logging.debug(f"Searching for posts by {artist} that are no older than {max_post_age_days} days")
    if not include_favorites_in_search:
        artist_search_tags.append(f"-fav:{api.username}")
        favorite_count = 0 # This is to prevent the artist from being penalized for posts that the user has favorited
        logging.debug(f"Excluding posts favorited by {api.username}")

    artist_search = api.list_posts(artist_search_tags)
    return get_metrics_for_search(artist_search, favorite_count, desired_tags_include, avoided_tags_include)

def calculate_respective_relevancy_scores(metric_dict:dict) -> dict:
    if len(metric_dict) == 0:
        return {}
    tracked_metrics_list = [
        "post_count",
        "mean_posts_score",
        "mean_posts_favcount",
        "mean_post_age_seconds",
        "matched_desired_tags_all_count",
        "matched_desired_tags_top_count",
        "matched_avoided_tags_all_count",
        "matched_avoided_tags_top_count",
        "unfavorited_post_percentage",
        "top_tags_count",
        "all_tags_count",
        "desired_tags_count",
        "avoided_tags_count",
    ]
    relevancy_scores = {}
    min_max_values = {
        metric: {"min": min(metrics[metric] for metrics in metric_dict.values()), "max": max(metrics[metric] for metrics in metric_dict.values())} for metric in tracked_metrics_list
    }
    for artist, metrics in metric_dict.items():
        relevancy_scores[artist] = calculate_relevancy_score(metrics, min_max_values)
    return relevancy_scores

def get_underappreciated_favorited_artists(
        api:ImageBoardApi,
        target_username:str="",
        max_post_exposure:int=5,
        desired_tags_percentile:float=.75,
        disregarded_common_tags_percentile:float=.9998,
        avoided_tags_include:list=None,
        artist_blacklist:list=None,
        sort_by_metric:str="relevance_score",
        ascending:bool=False
    ) -> list:
    def determine_if_underappreciated(
            artists_metrics:dict,
            min_mean_posts_score:int=75,
            min_post_count:int=15,
            min_relevance_score:int=.66,
            max_percent_favorited:float=.25,
        ):
        conditions = [
            (artists_metrics["unfavorited_post_percentage"] >= (1 - max_percent_favorited), f"Unfavorited percentage {artists_metrics["unfavorited_post_percentage"]} is less than threshold {(1 - max_percent_favorited)}"),
            (artists_metrics["mean_posts_score"] < min_mean_posts_score, f"Mean score {artists_metrics["mean_posts_score"]} is greater than threshold {min_mean_posts_score}"),
            (artists_metrics["post_count"] >= min_post_count, f"Post count {artists_metrics["post_count"]} is less than threshold {min_post_count}"),
            (artists_metrics["relevance_score"] >= min_relevance_score, f"Relevance score {artists_metrics["relevance_score"]} is less than threshold {min_relevance_score}")
        ]

        pass_status = True
        for condition, failure_reason in conditions:
            if not condition:
                pass_status = False
                logging.debug(f"{artist} failed condition: {failure_reason}")

        return pass_status
    if target_username == "":
        target_username = api.username

    favorite_posts = api.list_posts([f"fav:{target_username}"])
    favorite_posts_artist_counts =  post_tag_counts_combined(favorite_posts, ["artist"])
    favorite_posts_artist_counts.sort()
    favorite_posts_artist_counts = ImageBoardValueBlacklist(constants.NONARTIST_ARTISTS)(favorite_posts_artist_counts)
    all_general_tags = api.list_tags(hide_empty=True, category=constants.GENERAL, order="count")
    all_general_tag_counts = get_tag_post_counts(all_general_tags)
    print(len(all_general_tag_counts), 0)
    all_lore_tags = api.list_tags(hide_empty=True, category=constants.LORE, order="count")
    all_lore_tag_counts = get_tag_post_counts(all_lore_tags)
    print(len(all_lore_tag_counts), 1)
    all_tag_counts = all_general_tag_counts + all_lore_tag_counts
    all_tag_counts.sort()
    print(len(all_tag_counts), 2)
    # all_tag_counts = ImageBoardCountFilterTop(5000)(all_tag_counts)
    too_general_tags = ImageBoardCountFilterPercentile(disregarded_common_tags_percentile)(all_tag_counts)
    print(len(too_general_tags), 3)
    print("Index,Tag,Count")
    for i, (tag, count) in enumerate(too_general_tags):
        print(f"{i},{tag},{count}")
    preferred_nonartist_tag_counts = post_tag_counts_combined(favorite_posts, ["general", "lore"])
    preferred_nonartist_tag_counts.sort()
    preferred_nonartist_tag_counts = ImageBoardCountFilterPercentile(desired_tags_percentile)(preferred_nonartist_tag_counts)
    preferred_nonartist_tag_counts.sort()
    preferred_nonartist_tags = preferred_nonartist_tag_counts.values

    for general_tag in too_general_tags.values:
        if general_tag in preferred_nonartist_tags:
            preferred_nonartist_tags.remove(general_tag)


    logging.info(f"Found {len(favorite_posts_artist_counts)} favorite artists with {max_post_exposure} or fewer favorited posts by {target_username}")
    artist_metrics = {}
    for i, artist_tuple in enumerate(favorite_posts_artist_counts):
        artist = artist_tuple[0]
        user_fav_count = artist_tuple[1]
        logging.debug(f"{i}: {artist} has {user_fav_count} favorited posts by {target_username}")
        if artist in artist_blacklist:
            logging.debug(f"{artist} is blacklisted")
            continue
        artist_search = api.list_posts([artist])
        metrics = get_metrics_for_search(artist_search, preferred_nonartist_tags, avoided_tags_include)
        artist_post_count = metrics["post_count"]
        unfavorited_post_count = artist_post_count - user_fav_count
        unfavorited_post_percentage = unfavorited_post_count / artist_post_count
        metrics["unfavorited_post_count"] = unfavorited_post_count
        metrics["favorited_post_count"] = user_fav_count
        metrics["unfavorited_post_percentage"] = unfavorited_post_percentage
        artist_metrics[artist] = metrics

    artist_relevancy_scores = calculate_respective_relevancy_scores(artist_metrics)
    for artist, metrics in artist_metrics.items():
        print(f"{artist}: {artist_relevancy_scores[artist]}")
        artist_metrics[artist]["relevance_score"] = artist_relevancy_scores.get(artist, 0)

    underappreciated_artists = []
    for artist, metrics in artist_metrics.items():
        if determine_if_underappreciated(metrics):
            underappreciated_artists.append((artist, metrics))
    underappreciated_artists.sort(key=lambda x: x[1][sort_by_metric], reverse=not ascending)
    return underappreciated_artists

# def get_recent_posts_of_favorited_artists(api, target_username:str="", min_post_exposure:int=15):
