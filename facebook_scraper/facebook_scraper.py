import itertools
import logging
import warnings
from functools import partial
from typing import Iterator, Union

from requests import RequestException
from requests_html import HTMLSession

from . import utils
from .constants import DEFAULT_PAGE_LIMIT, FB_MOBILE_BASE_URL
from .extractors import extract_group_post, extract_post
from .fb_types import Post
from .page_iterators import iter_group_pages, iter_pages


logger = logging.getLogger(__name__)


class FacebookScraper:
    """Class for creating FacebookScraper Iterators"""

    base_url = FB_MOBILE_BASE_URL
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/76.0.3809.87 Safari/537.36"
    )
    default_headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.5',
    }

    def __init__(self, session=None, requests_kwargs=None):
        if session is None:
            session = HTMLSession()
            session.headers.update(self.default_headers)

        if requests_kwargs is None:
            requests_kwargs = {}

        self.session = session
        self.requests_kwargs = requests_kwargs

    def get_posts(self, account: str, **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_pages, account=account, request_fn=self.get)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_group_posts(self, group: Union[str, int], **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_group_pages, group=group, request_fn=self.get)
        return self._generic_get_posts(extract_group_post, iter_pages_fn, **kwargs)

    def get(self, url, **kwargs):
        try:
            response = self.session.get(url=url, **self.requests_kwargs, **kwargs)
            response.raise_for_status()
            return response
        except RequestException as ex:
            logger.exception("Exception while requesting URL: %s\nException: %r", url, ex)
            raise

    def login(self, email: str, password: str):
        login_page = self.get(self.base_url)
        login_action = login_page.html.find('#login_form', first=True).attrs.get('action')

        response = self.session.post(
            utils.urljoin(self.base_url, login_action), data={'email': email, 'pass': password}
        )
        response_text = response.html.find('#viewport', first=True).text

        logger.debug("Login response text: %s", response_text)

        login_error = response.html.find('#login_error', first=True)
        if login_error:
            logger.error("Login error: %s", login_error.text)

        if 'c_user' not in self.session.cookies:
            warnings.warn('login unsuccessful')

    def is_logged_in(self) -> bool:
        response = self.get('https://m.facebook.com/settings')
        return not response.url.startswith('https://m.facebook.com/login.php')

    def _generic_get_posts(
        self,
        extract_post_fn,
        iter_pages_fn,
        page_limit=DEFAULT_PAGE_LIMIT,
        options=None,
        remove_source=True,
    ):
        counter = itertools.count(0) if page_limit is None else range(page_limit)

        if options is None:
            options = {}
        elif isinstance(options, set):
            warnings.warn("The options argument should be a dictionary.", stacklevel=3)
            options = {k: True for k in options}

        if page_limit and page_limit <= 2:
            warnings.warn(
                "A low page limit (<=2) might return no results, try increasing the limit",
                stacklevel=3,
            )

        logger.debug("Starting to iterate pages")
        for i, page in zip(counter, iter_pages_fn()):
            logger.debug("Extracting posts from page %s", i)
            for post_element in page:
                post = extract_post_fn(post_element, options=options, request_fn=self.get)
                if remove_source:
                    post.pop('source', None)
                yield post
