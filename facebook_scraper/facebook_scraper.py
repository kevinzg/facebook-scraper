import json
import re
import time
import warnings

from requests import RequestException
from requests_html import HTML, HTMLSession

__all__ = ['get_posts', 'write_posts_to_csv']


_base_url = 'https://m.facebook.com'
_user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/76.0.3809.87 Safari/537.36")
_cookie = ('locale=en_US;')
_headers = {'User-Agent': _user_agent, 'Accept-Language': 'en-US,en;q=0.5', 'cookie': _cookie}

_session = None
_timeout = None

_cursor_regex = re.compile(r'href:"(/page_content[^"]+)"')  # First request
_cursor_regex_2 = re.compile(r'href":"(\\/page_content[^"]+)"')  # Other requests
_cursor_regex_3 = re.compile(r'\shref="(\/groups\/[^"]+bac=[^"]+)"')  # for Group requests


def get_posts(account=None, group=None, **kwargs):
    valid_args = sum(arg is not None for arg in (account, group))

    if valid_args != 1:
        raise ValueError("You need to specify either account or group")

    if account is not None:
        path = f'{account}/posts/'
        return _get_page_posts(path, **kwargs)

    elif group is not None:
        path = f'groups/{group}/'
        return _get_group_posts(path, **kwargs)


def _get_page_posts(path, pages=10, timeout=5, sleep=0, credentials=None, extra_info=False):
    """Gets posts for a given account."""
    global _session, _timeout

    url = f'{_base_url}/{path}'

    _session = HTMLSession()
    _session.headers.update(_headers)

    if credentials:
        _login_user(*credentials)

    _timeout = timeout

    response = _session.get(url, timeout=_timeout)
    html = HTML(html=response.html.html.replace('<!--', '').replace('-->', ''))
    cursor_blob = html.html

    while True:
        for article in html.find('article'):
            post = _extract_post(article)
            if extra_info:
                post = fetch_share_and_reactions(post)
            yield post

        pages -= 1
        if pages <= 0:
            return

        cursor = _find_cursor(cursor_blob)
        next_url = f'{_base_url}{cursor}'

        if sleep:
            time.sleep(sleep)

        try:
            response = _session.get(next_url, timeout=timeout)
            response.raise_for_status()
            data = json.loads(response.text.replace('for (;;);', '', 1))
        except (RequestException, ValueError):
            return

        for action in data['payload']['actions']:
            if action['cmd'] == 'replace':
                html = HTML(html=action['html'], url=_base_url)
            elif action['cmd'] == 'script':
                cursor_blob = action['code']


def _get_group_posts(path, pages=10, timeout=5, sleep=0, credentials=None, extra_info=False):
    """Gets posts for a given account."""
    global _session, _timeout

    url = f'{_base_url}/{path}'

    _session = HTMLSession()
    _session.headers.update(_headers)

    if credentials:
        _login_user(*credentials)

    _timeout = timeout

    while True:
        response = _session.get(url, timeout=_timeout)
        response.raise_for_status()
        html = HTML(html=response.html.html.replace('<!--', '').replace('-->', ''))
        cursor_blob = html.html

        for article in html.find('article'):
            post = _extract_post(article)
            if extra_info:
                post = fetch_share_and_reactions(post)
            yield post

        pages -= 1
        if pages <= 0:
            return

        cursor = _find_cursor(cursor_blob)

        if cursor is not None:
            url = f'{_base_url}{cursor}'

        if sleep:
            time.sleep(sleep)


def _find_cursor(text):
    match = _cursor_regex.search(text)
    if match:
        return match.groups()[0]

    match = _cursor_regex_2.search(text)
    if match:
        value = match.groups()[0]
        return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')

    match = _cursor_regex_3.search(text)
    if match:
        value = match.groups()[0]
        return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')

    return None


def _login_user(email, password):
    login_page = _session.get(_base_url)
    login_action = login_page.html.find('#login_form', first=True).attrs.get('action')
    _session.post(_base_url + login_action, data={'email': email, 'pass': password})
    if 'c_user' not in _session.cookies:
        warnings.warn('login unsuccessful')


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
