import itertools
import warnings
from functools import partial
from typing import Iterator

from requests_html import HTMLSession

from . import utils
from .constants import DEFAULT_PAGE_LIMIT, FB_MOBILE_BASE_URL
from .extractors import extract_post
from .page_iterators import iter_group_pages, iter_pages
from .typing import Post


class FacebookScraper:
    base_url = FB_MOBILE_BASE_URL
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/76.0.3809.87 Safari/537.36"
    )
    cookie = 'locale=en_US;'
    default_headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.5',
        'cookie': cookie,
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

    def get_group_posts(self, group: str, **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_group_pages, group=group, request_fn=self.get)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get(self, url, **kwargs):
        return self.session.get(url=url, **self.requests_kwargs, **kwargs)

    def login(self, email, password):
        login_page = self.get(self.base_url)
        login_action = login_page.html.find('#login_form', first=True).attrs.get('action')
        self.session.post(
            utils.urljoin(self.base_url, login_action), data={'email': email, 'pass': password}
        )

        if 'c_user' not in self.session.cookies:
            warnings.warn('login unsuccessful')

    def _generic_get_posts(
        self, extract_post_fn, iter_pages_fn, page_limit=DEFAULT_PAGE_LIMIT, extra_info=False
    ):
        counter = itertools.count(0) if page_limit is None else range(page_limit)

        options = set()
        if extra_info:
            options.add('reactions')

        for page, _ in zip(iter_pages_fn(), counter):
            for post_element in page:
                yield extract_post_fn(post_element, options=options, request_fn=self.get)


def write_posts_to_csv(account=None, group=None, filename=None, **kwargs):
    """
    :param account:     Facebook account name e.g. "nike", string
    :param group:       Facebook group id
    :param filename:    File name, defaults to <<account_posts.csv>>
    :param pages:       Number of pages to scan, integer
    :param timeout:     Session response timeout in seconds, integer
    :param sleep:       Sleep time in s before every call, integer
    :param credentials: Credentials for login - username and password, tuple
    :return:            CSV written in the same location with <<account_name>>_posts.csv
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


def _main():
    """facebook-scraper entry point when used as a script"""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('account', type=str, help="Facebook account")
    parser.add_argument('-f', '--filename', type=str, help="Output filename")
    parser.add_argument('-p', '--pages', type=int, help="Number of pages to download", default=10)

    args = parser.parse_args()

    write_posts_to_csv(account=args.account, filename=args.filename, pages=args.pages)


if __name__ == '__main__':
    _main()
