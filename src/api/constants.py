MAX_PAGE_SIZE_PER_REQUEST:int = 320
MAX_REQUESTS_PER_MINUTE:int = 120
MAX_TAGS_PER_REQUEST:int = 40
NONARTIST_ARTISTS:list = [
    "conditional_dnp",
    "unknown_artist",
    "epilepsy_warning",
    "anonymous_artist",
    "unknown_artist_signature",
    "avoid_posting",
    "sound_warning",
    "jumpscare_warning",
    "third-party_edit"
]
URL_SUFFIX:str = ".json"
FAVORITES:str = "favorites"
POSTS:str = "posts"
POST_FLAGS:str = "post_flags"
NOTES:str = "notes"
TAGS:str = "tags"
TAG_ALIASES:str = "tag_aliases"

GENERAL:int = 0
ARTIST:int = 1
COPYRIGHT:int = 3
CHARACTER:int = 4
SPECIES:int = 5
INVALID:int = 6
META:int = 7
LORE:int = 8
TAG_CATEGORY_NAMES:dict = {
    GENERAL: "general",
    ARTIST: "artist",
    COPYRIGHT: "copyright",
    CHARACTER: "character",
    SPECIES: "species",
    INVALID: "invalid",
    META: "meta",
    LORE: "lore"
}

EXPLICIT:str = "e"
QUESTIONABLE:str = "q"
SAFE:str = "s"
RATING_NAMES:dict = {
    EXPLICIT: "explicit",
    QUESTIONABLE: "questionable",
    SAFE: "safe"
}
