import csv
import logging
import sys
from typing import Iterator, Optional, Tuple, Union

from .constants import DEFAULT_REQUESTS_TIMEOUT
from .facebook_scraper import FacebookScraper
from .fb_types import Post, Credentials


_scraper = FacebookScraper()


def get_posts(
    account: Optional[str] = None,
    group: Union[str, int, None] = None,
    credentials: Optional[Credentials] = None,
    **kwargs,
) -> Iterator[Post]:
    """Get posts from a Facebook page or group.

    Args:
        account (str): The account of the page.
        group (int): The group id.
        credentials (Optional[Tuple[str, str]]): Tuple of email and password to login before scraping.
        timeout (int): Timeout for requests.
        page_limit (int): How many pages of posts to go through.
            Use None to try to get all of them.
        extra_info (bool): Set to True to try to get reactions.
        youtube_dl (bool): Use Youtube-DL for video extraction.

    Yields:
        dict: The post representation in a dictionary.
    """
    valid_args = sum(arg is not None for arg in (account, group))

    if valid_args != 1:
        raise ValueError("You need to specify either account or group")

    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)

    options = kwargs.setdefault('options', set())

    # TODO: Deprecate `pages` in favor of `page_limit` since it is less confusing
    if 'pages' in kwargs:
        kwargs['page_limit'] = kwargs.pop('pages')

    # TODO: Deprecate `extra_info` in favor of `options`
    extra_info = kwargs.pop('extra_info', False)
    if extra_info:
        options.add('reactions')
    if kwargs.pop('youtube_dl', False):
        options.add('youtube_dl')

    if credentials is not None:
        _scraper.login(*credentials)

    if account is not None:
        return _scraper.get_posts(account, **kwargs)

    elif group is not None:
        return _scraper.get_group_posts(group, **kwargs)


def write_posts_to_csv(
    account: Optional[str] = None,
    group: Union[str, int, None] = None,
    filename: str = None,
    **kwargs,
):
    """Write posts from an account or group to a CSV file

    Args:
        account (str): Facebook account name e.g. "nike" or "nintendo"
        group (Union[str, int, None]): Facebook group id e.g. 676845025728409 
        filename (str): Filename, defaults to <account or group>_posts.csv
        credentials (Optional[Tuple[str, str]]): Tuple of email and password to login before scraping. Defaults to scrape anonymously
        timeout (Optional[int]): Timeout for requests.
        page_limit (Optional[int]): How many pages of posts to go through.
            Use None to try to get all of them.
        extra_info (Optional[bool]): Set to True to try to get reactions.
    """
    list_of_posts = list(get_posts(account=account, group=group, **kwargs))

    if not list_of_posts:
        print("Couldn't get any posts.", file=sys.stderr)
        return

    keys = list_of_posts[0].keys()

    if filename is None:
        filename = str(account or group) + "_posts.csv"

    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(list_of_posts)


def enable_logging(level=logging.INFO):
    handler = logging.StreamHandler()
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)


# Disable logging by default
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
