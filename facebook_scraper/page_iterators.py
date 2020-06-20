import json
import logging
import re
import textwrap
from typing import Iterator, Optional

from . import utils
from .constants import FB_MOBILE_BASE_URL
from .fb_types import URL, RawPage, Page, RequestFunction, Response


logger = logging.getLogger(__name__)


def iter_pages(account: str, request_fn: RequestFunction) -> Iterator[Page]:
    start_url = utils.urljoin(FB_MOBILE_BASE_URL, f'/{account}/posts/')
    return generic_iter_pages(start_url, PageParser, request_fn)


def iter_group_pages(group: str, request_fn: RequestFunction) -> Iterator[Page]:
    start_url = utils.urljoin(FB_MOBILE_BASE_URL, f'groups/{group}/')
    return generic_iter_pages(start_url, GroupPageParser, request_fn)


def generic_iter_pages(start_url, page_parser_cls, request_fn: RequestFunction) -> Iterator[Page]:
    next_url = start_url

    while next_url:
        logger.debug("Requesting page from: %s", next_url)
        response = request_fn(next_url)

        logger.debug("Parsing page response")
        parser = page_parser_cls(response)

        page = parser.get_page()

        # TODO: If page is actually an iterable calling len(page) might consume it
        logger.debug("Got %s raw posts from page", len(page))
        yield page

        logger.debug("Looking for next page URL")
        next_page = parser.get_next_page()
        if next_page:
            next_url = utils.urljoin(FB_MOBILE_BASE_URL, next_page)
        else:
            logger.info("Page parser did not find next page URL")
            next_url = None


class PageParser:
    json_prefix = 'for (;;);'

    cursor_regex = re.compile(r'href:"(/page_content[^"]+)"')  # First request
    cursor_regex_2 = re.compile(r'href":"(\\/page_content[^"]+)"')  # Other requests

    def __init__(self, response: Response):
        self.response = response
        self.html = None
        self.cursor_blob = None

        self._parse()

    def get_page(self) -> Page:
        raw_page = self.get_raw_page()
        raw_posts = raw_page.find('article')

        if not raw_posts:
            logger.warning("No raw posts (<article> elements) were found in this page.")
            if logger.isEnabledFor(logging.DEBUG):
                content = textwrap.indent(
                    utils.html2text(raw_page.html),
                    prefix='| ',
                    predicate=lambda _: True,
                )
                sep = '+' + '-' * 60
                logger.debug("The page url is: %s", self.response.url)
                logger.debug("The page content is:\n%s\n%s%s\n", sep, content, sep)

        return raw_posts

    def get_raw_page(self) -> RawPage:
        return self.html

    def get_next_page(self) -> Optional[URL]:
        match = self.cursor_regex.search(self.cursor_blob)
        if match:
            return match.groups()[0]

        match = self.cursor_regex_2.search(self.cursor_blob)
        if match:
            value = match.groups()[0]
            return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')

        return None

    def _parse(self):
        if self.response.text.startswith(self.json_prefix):
            self._parse_json()
        else:
            self._parse_html()

    def _parse_html(self):
        # TODO: Why are we uncommenting HTML?
        self.html = utils.make_html_element(
            self.response.text.replace('<!--', '').replace('-->', ''), url=self.response.url,
        )
        self.cursor_blob = self.response.text

    def _parse_json(self):
        prefix_length = len(self.json_prefix)
        data = json.loads(self.response.text[prefix_length:])  # Strip 'for (;;);'

        for action in data['payload']['actions']:
            if action['cmd'] == 'replace':
                self.html = utils.make_html_element(action['html'], url=FB_MOBILE_BASE_URL)
            elif action['cmd'] == 'script':
                self.cursor_blob = action['code']

        assert self.html is not None
        assert self.cursor_blob is not None


class GroupPageParser(PageParser):
    cursor_regex_3 = re.compile(r'\shref="(\/groups\/[^"]+bac=[^"]+)"')  # for Group requests

    def get_next_page(self) -> Optional[URL]:
        next_page = super().get_next_page()
        if next_page:
            return next_page

        match = self.cursor_regex_3.search(self.cursor_blob)
        if match:
            value = match.groups()[0]
            return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')

        return None

    def _parse(self):
        self._parse_html()
