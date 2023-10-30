import csv
import json
import locale
import logging
import pathlib
import sys
import warnings
import pickle
from typing import Any, Dict, Iterator, Optional, Set, Union

from requests.cookies import cookiejar_from_dict

from .constants import DEFAULT_REQUESTS_TIMEOUT, DEFAULT_COOKIES_FILE_PATH
from .facebook_scraper import FacebookScraper
from .fb_types import Credentials, Post, RawPost, Profile
from .utils import html_element_to_string, parse_cookie_file
from . import exceptions
import traceback
import time
from datetime import datetime, timedelta
import re
import os


_scraper = FacebookScraper()


def set_cookies(cookies):
    if isinstance(cookies, str):
        if cookies == "from_browser":
            try:
                import browser_cookie3

                cookies = browser_cookie3.load(domain_name='.facebook.com')
            except:
                raise ModuleNotFoundError(
                    "browser_cookie3 must be installed to use browser cookies"
                )
        else:
            try:
                cookies = parse_cookie_file(cookies)
            except ValueError as e:
                raise exceptions.InvalidCookies(f"Cookies are in an invalid format: {e}")
    elif isinstance(cookies, dict):
        cookies = cookiejar_from_dict(cookies)
    if cookies is not None:
        cookie_names = [c.name for c in cookies]
        missing_cookies = [c for c in ['c_user', 'xs'] if c not in cookie_names]
        if missing_cookies:
            raise exceptions.InvalidCookies(f"Missing cookies with name(s): {missing_cookies}")
        _scraper.session.cookies.update(cookies)
        if not _scraper.is_logged_in():
            raise exceptions.InvalidCookies(f"Cookies are not valid")


def unset_cookies():
    # Explicitly unset cookies to return to unauthenticated requests
    _scraper.session.cookies = cookiejar_from_dict({})


def set_proxy(proxy, verify=True):
    _scraper.set_proxy(proxy, verify)


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


def get_reactors(
    post_id: Union[str, int],
    **kwargs,
) -> Iterator[dict]:
    """Get reactors for a given post ID
    Args:
        post_id(str): The post ID, as returned from get_posts
        cookies (Union[dict, CookieJar, str]): Cookie jar to use.
            Can also be a filename to load the cookies from a file (Netscape format).
    """
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_reactors(post_id, **kwargs)


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
    hashtag: Optional[str] = None,
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
    valid_args = sum(arg is not None for arg in (account, group, post_urls, hashtag))

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

    elif hashtag is not None:
        return _scraper.get_posts_by_hashtag(hashtag, **kwargs)

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


def get_posts_by_search(
    word: str,
    credentials: Optional[Credentials] = None,
    **kwargs,
) -> Iterator[Post]:
    """Get posts by searching all of Facebook
    Args:
        word (str): The word for searching posts.
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
    if not word:
        raise ValueError("You need to specify word")

    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)

    cookies = kwargs.pop('cookies', None)

    if cookies is not None and credentials is not None:
        raise ValueError("Can't use cookies and credentials arguments at the same time")
    set_cookies(cookies)

    options: Union[Dict[str, Any], Set[str]] = kwargs.setdefault('options', {})
    if isinstance(options, set):
        warnings.warn("The options argument should be a dictionary.", stacklevel=2)
        options = {k: True for k in options}

    options.setdefault('word', word)

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

    if word is not None:
        return _scraper.get_posts_by_search(word, **kwargs)

    raise ValueError('No account nor group')


def write_post_to_disk(post: Post, source: RawPost, location: pathlib.Path):
    post_id = post['post_id']
    filename = f'{post_id}.html'

    logger.debug("Writing post %s", post_id)
    with open(location.joinpath(filename), mode='wt') as f:
        f.write('<!--\n')
        json.dump(post, f, indent=4, default=str)
        f.write('\n-->\n')
        f.write(html_element_to_string(source, pretty=True))


def write_posts_to_csv(
    account: Optional[str] = None,
    group: Union[str, int, None] = None,
    filename: str = None,
    encoding: str = None,
    **kwargs,
):
    """Write posts from an account or group to a CSV or JSON file

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
    dump_location = kwargs.pop('dump_location', None)  # For dumping HTML to disk, for debugging
    if dump_location is not None:
        dump_location.mkdir(exist_ok=True)
        kwargs["remove_source"] = False

    # Set a default filename, based on the account name with the appropriate extension
    if filename is None:
        filename = str(account or group) + "_posts." + kwargs.get("format")

    if encoding is None:
        encoding = locale.getpreferredencoding()

    if os.path.isfile(filename):
        raise FileExistsError(f"{filename} exists")

    if filename == "-":
        output_file = sys.stdout
    else:
        output_file = open(filename, 'w', newline='', encoding=encoding)

    first_post = True

    sleep = kwargs.pop("sleep", 0)

    days_limit = kwargs.get("days_limit", 3650)
    max_post_time = datetime.now() - timedelta(days=days_limit)

    start_url = None
    resume_file = kwargs.get("resume_file")
    if resume_file:
        try:
            with open(resume_file, "r") as f:
                existing_url = f.readline().strip()
            logger.debug("Existing URL:" + existing_url)
            if existing_url:
                start_url = existing_url
        except FileNotFoundError:
            pass

    def handle_pagination_url(url):
        if resume_file:
            with open(resume_file, "w") as f:
                f.write(url + "\n")

    keys = kwargs.get("keys")

    try:
        for post in get_posts(
            account=account,
            group=group,
            start_url=start_url,
            request_url_callback=handle_pagination_url,
            **kwargs,
        ):
            if dump_location is not None:
                source = post.pop('source')
                try:
                    write_post_to_disk(post, source, dump_location)
                except Exception:
                    logger.exception("Error writing post to disk")
            elif post.get("source"):
                post["source"] = post["source"].html
            if first_post:
                if kwargs.get("format") == "json":
                    output_file.write("[\n")
                else:
                    if not keys:
                        keys = list(post.keys())
                    dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore')
                    dict_writer.writeheader()
            else:
                if kwargs.get("format") == "json":
                    output_file.write(",")
            match = None
            if post["text"]:
                match = re.search(kwargs.get("matching", '.+'), post["text"], flags=re.IGNORECASE)
                if kwargs.get("not_matching") and re.search(
                    kwargs.get("not_matching"), post["text"], flags=re.IGNORECASE
                ):
                    match = None
            if match:
                if kwargs.get("format") == "json":
                    if keys:
                        post = {k: v for k, v in post.items() if k in keys}
                    json.dump(post, output_file, default=str, indent=4)
                else:
                    dict_writer.writerow(post)
            if not first_post and post["time"] and post["time"] < max_post_time:
                logger.debug(
                    f"Reached days_limit - {post['time']} is more than {days_limit} days old (older than {max_post_time})"
                )
                break
            first_post = False
            time.sleep(sleep)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        traceback.print_exc()

    if kwargs.get("format") == "json":
        output_file.write("\n]")
    if first_post:
        print("Couldn't get any posts.", file=sys.stderr)
    output_file.close()


def get_groups_by_search(
    word: str,
    **kwargs,
):
    """Searches Facebook groups and yields ids for each result
    on the first page"""
    _scraper.requests_kwargs['timeout'] = kwargs.pop('timeout', DEFAULT_REQUESTS_TIMEOUT)
    cookies = kwargs.pop('cookies', None)
    set_cookies(cookies)
    return _scraper.get_groups_by_search(word, **kwargs)


def enable_logging(level=logging.DEBUG):
    handler = logging.StreamHandler()
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)


def use_persistent_session(email: str, password: str, cookies_file_path=DEFAULT_COOKIES_FILE_PATH):
    """Login persistently to Facebook and save cookies to a file (default: ".fb-cookies.pckl"). This is highly recommended if you want to scrape several times a day because it will keep your session alive instead of logging in every time (which can be flagged as suspicious by Facebook).

    Args:
        email (str): email address to login.
        password (str): password to login.
        cookies_file_path (str, optional): path to the file in which to save cookies. Defaults to ".fb-cookies.pckl".

    Raises:
        exceptions.InvalidCredentials: if the credentials are invalid.

    Returns:
        Boolean: True if the login was successful, False otherwise.
    """
    try:
        with open(cookies_file_path, "rb") as f:
            cookies = pickle.load(f)
        logger.debug("Loaded cookies from %s", cookies_file_path)
    except FileNotFoundError:
        logger.error("No cookies file found at %s", cookies_file_path)
        cookies = None
    try:
        if not cookies:
            raise exceptions.InvalidCookies()
        set_cookies(cookies)
        logger.debug("Successfully logged in with cookies")
    except exceptions.InvalidCookies:
        logger.exception("Invalid cookies, trying to login with credentials")
        _scraper.login(email, password)
        cookies = _scraper.session.cookies
        with open(cookies_file_path, "wb") as f:
            pickle.dump(cookies, f)
        set_cookies(cookies)
        logger.debug("Successfully logged in with credentials")


# Disable logging by default
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
