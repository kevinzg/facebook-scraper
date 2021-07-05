import csv
import json
import locale
import logging
import pathlib
import sys
import warnings
from typing import Any, Dict, Iterator, Optional, Set, Union

from requests.cookies import cookiejar_from_dict

from .constants import DEFAULT_REQUESTS_TIMEOUT
from .facebook_scraper import FacebookScraper
from .fb_types import Credentials, Post, RawPost, Profile
from .utils import html_element_to_string, parse_cookie_file
from . import exceptions


_scraper = FacebookScraper()


def set_cookies(cookies):
    if isinstance(cookies, str):
        try:
            cookies = parse_cookie_file(cookies)
        except ValueError as e:
            raise exceptions.InvalidCookies(f"Cookies are in an invalid format: {e}")
    elif isinstance(cookies, dict):
        cookies = cookiejar_from_dict(cookies)
    if cookies is not None:
        missing_cookies = [c for c in ['c_user', 'xs'] if c not in cookies]
        if missing_cookies:
            raise exceptions.InvalidCookies(f"Missing cookies with name(s): {missing_cookies}")
        _scraper.session.cookies.update(cookies)
        if not _scraper.is_logged_in():
            raise exceptions.InvalidCookies(f"Cookies are not valid")


def unset_cookies():
    # Explicitly unset cookies to return to unauthenticated requests
    _scraper.session.cookies = cookiejar_from_dict({})


def set_proxy(proxy):
    _scraper.set_proxy(proxy)


def set_user_agent(user_agent):
    _scraper.set_user_agent(user_agent)


def set_noscript(noscript):
    _scraper.set_noscript(noscript)


def get_profile(
    account: str,
    **kwargs,
) -> Profile:
    """Get a Facebook user's profile information
    Args:
        account(str): The account of the profile.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_profile(account, **kwargs)


def get_friends(
    account: str,
    **kwargs,
) -> Iterator[Profile]:
    """Get a Facebook user's friends
    Args:
        account(str): The account of the profile.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_friends(account, **kwargs)


def get_page_info(account: str, **kwargs) -> Profile:
    """Get a page's information
    Args:
        account(str): The account of the profile.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_page_info(account, **kwargs)


def get_group_info(group: Union[str, int], **kwargs) -> Profile:
    """Get a group's profile information
    Args:
        group(str or int): The group name or ID
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_group_info(group, **kwargs)


def get_shop(account: str, **kwargs) -> Iterator[Post]:
    """Get a page's shop listings
    Args:
        account(str): The account of the profile.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_shop(account, **kwargs)


def get_posts(
    account: Optional[str] = None,
    group: Union[str, int, None] = None,
    post_urls: Optional[Iterator[str]] = None,
    credentials: Optional[Credentials] = None,
    **kwargs,
) -> Iterator[Post]:
    """Get posts from a Facebook page or group.

    Args:
        account (str): The account of the page.
        group (int): The group id.
        post_urls ([str]): List of manually specified post URLs.
        credentials (Optional[Tuple[str, str]]): Tuple of email and password to login before scraping.
        timeout (int): Timeout for requests.
        page_limit (int): How many pages of posts to go through.
            Use None to try to get all of them.
        extra_info (bool): Set to True to try to get reactions.
        youtube_dl (bool): Use Youtube-DL for video extraction.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).

    Yields:
        dict: The post representation in a dictionary.
    """
    valid_args = sum(arg is not None for arg in (account, group, post_urls))

    if valid_args != 1:
        raise ValueError("You need to specify either account, group, or post_urls")

    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)

    cookies = kwargs.pop('cookies', None)

    if cookies is not None and credentials is not None:
        raise ValueError("Can't use cookies and credentials arguments at the same time")
    set_cookies(cookies)

    options: Union[Dict[str, Any], Set[str]] = kwargs.setdefault('options', {})
    if isinstance(options, set):
        warnings.warn("The options argument should be a dictionary.", stacklevel=2)
        options = {k: True for k in options}
    options.setdefault('account', account)

    # TODO: Add a better throttling mechanism
    if 'sleep' in kwargs:
        warnings.warn(
            "The sleep parameter has been removed, it won't have any effect.", stacklevel=2
        )
        kwargs.pop('sleep')

    # TODO: Deprecate `pages` in favor of `page_limit` since it is less confusing
    if 'pages' in kwargs:
        kwargs['page_limit'] = kwargs.pop('pages')

    # TODO: Deprecate `extra_info` in favor of `options`
    if "reactions" not in options:
        options['reactions'] = kwargs.pop('extra_info', False)
    options['youtube_dl'] = kwargs.pop('youtube_dl', False)

    if credentials is not None:
        _scraper.login(*credentials)

    if account is not None:
        return _scraper.get_posts(account, **kwargs)

    elif group is not None:
        return _scraper.get_group_posts(group, **kwargs)

    elif post_urls is not None:
        return _scraper.get_posts_by_url(post_urls, **kwargs)

    raise ValueError('No account nor group')


def get_photos(
    account: str,
    credentials: Optional[Credentials] = None,
    **kwargs,
) -> Iterator[Post]:
    """Get photo posts from a Facebook page.

    Args:
        account (str): The account of the page.
        credentials (Optional[Tuple[str, str]]): Tuple of email and password to login before scraping.
        timeout (int): Timeout for requests.
        page_limit (int): How many pages of posts to go through.
            Use None to try to get all of them.
        extra_info (bool): Set to True to try to get reactions.
        youtube_dl (bool): Use Youtube-DL for video extraction.
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).

    Yields:
        dict: The post representation in a dictionary.
    """
    if account is None:
        raise ValueError("You need to specify account")

    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)

    cookies = kwargs.pop('cookies', None)

    if cookies is not None and credentials is not None:
        raise ValueError("Can't use cookies and credentials arguments at the same time")
    set_cookies(cookies)

    options: Union[Dict[str, Any], Set[str]] = kwargs.setdefault('options', {})
    if isinstance(options, set):
        warnings.warn("The options argument should be a dictionary.", stacklevel=2)
        options = {k: True for k in options}
    options.setdefault('account', account)

    # TODO: Add a better throttling mechanism
    if 'sleep' in kwargs:
        warnings.warn(
            "The sleep parameter has been removed, it won't have any effect.", stacklevel=2
        )
        kwargs.pop('sleep')

    # TODO: Deprecate `pages` in favor of `page_limit` since it is less confusing
    if 'pages' in kwargs:
        kwargs['page_limit'] = kwargs.pop('pages')

    # TODO: Deprecate `extra_info` in favor of `options`
    options['reactions'] = kwargs.pop('extra_info', False)
    options['youtube_dl'] = kwargs.pop('youtube_dl', False)

    if credentials is not None:
        _scraper.login(*credentials)

    return _scraper.get_photos(account, **kwargs)


def write_posts_to_csv(
    account: Optional[str] = None,
    group: Union[str, int, None] = None,
    filename: str = None,
    encoding: str = None,
    **kwargs,
):
    """Write posts from an account or group to a CSV file

    Args:
        account (str): Facebook account name e.g. "nike" or "nintendo"
        group (Union[str, int, None]): Facebook group id e.g. 676845025728409
        filename (str): Filename, defaults to <account or group>_posts.csv
        encoding (str): Encoding for the output file, defaults to locale.getpreferredencoding()
        credentials (Optional[Tuple[str, str]]): Tuple of email and password to login before scraping. Defaults to scrape anonymously
        timeout (Optional[int]): Timeout for requests.
        page_limit (Optional[int]): How many pages of posts to go through.
            Use None to try to get all of them.
        extra_info (Optional[bool]): Set to True to try to get reactions.
        dump_location (Optional[pathlib.Path]): Location where to write the HTML source of the posts.
    """
    dump_location = kwargs.pop('dump_location', None)

    list_of_posts = list(
        get_posts(account=account, group=group, remove_source=not bool(dump_location), **kwargs)
    )

    if not list_of_posts:
        print("Couldn't get any posts.", file=sys.stderr)
        return

    def write_post_to_disk(post: Post, source: RawPost, location: pathlib.Path, index: int):
        post_id = post['post_id'] or f'no_id_{index}'
        filename = f'{post_id}.html'

        logger.debug("Writing post %s", post_id)
        with open(location.joinpath(filename), mode='wt') as f:
            f.write('<!--\n')
            json.dump(post, f, indent=4, default=str)
            f.write('\n-->\n')
            f.write(html_element_to_string(source, pretty=True))

    if dump_location is not None:
        dump_location.mkdir(exist_ok=True)

        for i, post in enumerate(list_of_posts):
            source = post.pop('source')
            try:
                write_post_to_disk(post, source, dump_location, i)
            except Exception:
                logger.exception("Error writing post to disk")

    keys = list(list_of_posts[0].keys())
    for post in list_of_posts:
        for key in post.keys():
            if key not in keys:
                keys.append(key)

    if filename is None:
        filename = str(account or group) + "_posts.csv"

    if encoding is None:
        encoding = locale.getpreferredencoding()

    with open(filename, 'w', newline='', encoding=encoding) as output_file:
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
