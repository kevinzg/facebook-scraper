import codecs
import itertools
import json
import re
import time
import warnings
from datetime import datetime
from urllib import parse as urlparse

from requests import RequestException
from requests_html import HTML, HTMLSession

__all__ = ['get_posts']


_base_url = 'https://m.facebook.com'
_user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/76.0.3809.87 Safari/537.36")
_cookie = ('locale=en_US;')
_headers = {'User-Agent': _user_agent, 'Accept-Language': 'en-US,en;q=0.5', 'cookie': _cookie}

_session = None
_timeout = None

_likes_regex = re.compile(r'like_def[^>]*>([0-9,.]+)')
_comments_regex = re.compile(r'cmt_def[^>]*>([0-9,.]+)')
_shares_regex = re.compile(r'([0-9,.]+)\s+Shares', re.IGNORECASE)
_link_regex = re.compile(r"href=\"https:\/\/lm\.facebook\.com\/l\.php\?u=(.+?)\&amp;h=")

_cursor_regex = re.compile(r'href:"(/page_content[^"]+)"')  # First request
_cursor_regex_2 = re.compile(r'href":"(\\/page_content[^"]+)"')  # Other requests

_photo_link = re.compile(r"href=\"(/[^\"]+/photos/[^\"]+?)\"")
_image_regex = re.compile(r"<a href=\"([^\"]+?)\" target=\"_blank\" class=\"sec\">View Full Size<\/a>", re.IGNORECASE)
_image_regex_lq = re.compile(r"background-image: url\('(.+)'\)")
_post_url_regex = re.compile(r'/story.php\?story_fbid=')

_more_url_regex = re.compile(r'(?<=…\s)<a href="([^"]+)')
_post_story_regex = re.compile(r'href="(\/story[^"]+)" aria')

_shares_and_reactions_regex = re.compile(
    r'<script>.*bigPipe.onPageletArrive\((?P<data>\{.*RelayPrefetchedStreamCache.*\})\);.*</script>')
_bad_json_key_regex = re.compile(r'(?P<prefix>[{,])(?P<key>\w+):')


def get_posts(account=None, group=None, **kwargs):
    valid_args = sum(arg is not None for arg in (account, group))
    if valid_args != 1:
        raise ValueError("You need to specify either account or group")

    if account is not None:
        path = f'{account}/posts/'
    elif group is not None:
        path = f'groups/{group}/'

    return _get_posts(path, **kwargs)


def _get_posts(path, pages=10, timeout=5, sleep=0, credentials=None, extra_info=False):
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
        if pages == 0:
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


def _extract_post(article):
    text, post_text, shared_text = _extract_text(article)
    return {
        'post_id': _extract_post_id(article),
        'text': text,
        'post_text': post_text,
        'shared_text': shared_text,
        'time': _extract_time(article),
        'image': _extract_image(article),
        'likes': _find_and_search(article, 'footer', _likes_regex, _parse_int) or 0,
        'comments': _find_and_search(article, 'footer', _comments_regex, _parse_int) or 0,
        'shares':  _find_and_search(article, 'footer', _shares_regex, _parse_int) or 0,
        'post_url': _extract_post_url(article),
        'link': _extract_link(article),
    }


def _extract_post_id(article):
    try:
        data_ft = json.loads(article.attrs['data-ft'])
        return data_ft['mf_story_key']
    except (KeyError, ValueError):
        return None


def _extract_text(article):
    # Open this article individually because not all content is fully loaded when skimming through pages
    # This ensures the full content can be read
    hasMore = _more_url_regex.search(article.html)
    if hasMore:
        match = _post_story_regex.search(article.html)
        if match:
            url = f'{_base_url}{match.groups()[0].replace("&amp;", "&")}'
            response = _session.get(url, timeout=_timeout)
            article = response.html.find('.story_body_container', first=True)

    nodes = article.find('p, header')
    if nodes:
        post_text = []
        shared_text = []
        ended = False
        for node in nodes[1:]:
            if node.tag == "header":
                ended = True

            # Remove '... More'
            # This button is meant to display the hidden text that is already loaded
            # Not to be confused with the 'More' that opens the article in a new page
            if node.tag == "p":
                node = HTML(html=node.html.replace('>… <', '><', 1).replace('>More<', '', 1))

            if not ended:
                post_text.append(node.text)
            else:
                shared_text.append(node.text)

        text = '\n'.join(itertools.chain(post_text, shared_text))
        post_text = '\n'.join(post_text)
        shared_text = '\n'.join(shared_text)

        return text, post_text, shared_text

    return None


def _extract_time(article):
    try:
        data_ft = json.loads(article.attrs['data-ft'])
        page_insights = data_ft['page_insights']
    except (KeyError, ValueError):
        return None

    for page in page_insights.values():
        try:
            timestamp = page['post_context']['publish_time']
            return datetime.fromtimestamp(timestamp)
        except (KeyError, ValueError):
            continue
    return None


def _extract_photo_link(article):
    match = _photo_link.search(article.html)
    if not match:
        return None

    url = f"{_base_url}{match.groups()[0]}"

    response = _session.get(url, timeout=_timeout)
    html = response.html.html
    match = _image_regex.search(html)
    if match:
        return match.groups()[0].replace("&amp;", "&")
    return None


def _extract_image(article):
    image_link = _extract_photo_link(article)
    if image_link is not None:
        return image_link
    return _extract_image_lq(article)


def _extract_image_lq(article):
    story_container = article.find('div.story_body_container', first=True)
    if story_container is None:
        return None
    other_containers = story_container.xpath('div/div')

    for container in other_containers:
        image_container = container.find('.img', first=True)
        if image_container is None:
            continue

        style = image_container.attrs.get('style', '')
        match = _image_regex_lq.search(style)
        if match:
            return _decode_css_url(match.groups()[0])

    return None


def _extract_link(article):
    html = article.html
    match = _link_regex.search(html)
    if match:
        return urlparse.unquote(match.groups()[0])
    return None


def _extract_post_url(article):
    query_params = ('story_fbid', 'id')

    elements = article.find('header a')
    for element in elements:
        href = element.attrs.get('href', '')
        match = _post_url_regex.match(href)
        if match:
            path = _filter_query_params(href, whitelist=query_params)
            return f'{_base_url}{path}'

    return None


def _find_and_search(article, selector, pattern, cast=str):
    container = article.find(selector, first=True)
    match = container and pattern.search(container.html)
    return match and cast(match.groups()[0])


def _find_cursor(text):
    match = _cursor_regex.search(text)
    if match:
        return match.groups()[0]

    match = _cursor_regex_2.search(text)
    if match:
        value = match.groups()[0]
        return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')

    return None


def _parse_int(value):
    return int(''.join(filter(lambda c: c.isdigit(), value)))


def _decode_css_url(url):
    url = re.sub(r'\\(..) ', r'\\x\g<1>', url)
    url, _ = codecs.unicode_escape_decode(url)
    return url


def _filter_query_params(url, whitelist=None, blacklist=None):
    def is_valid_param(param):
        if whitelist is not None:
            return param in whitelist
        if blacklist is not None:
            return param not in blacklist
        return True  # Do nothing

    parsed_url = urlparse.urlparse(url)
    query_params = urlparse.parse_qsl(parsed_url.query)
    query_string = urlparse.urlencode(
        [(k, v) for k, v in query_params if is_valid_param(k)]
    )
    return urlparse.urlunparse(parsed_url._replace(query=query_string))


def _login_user(email, password):
    login_page = _session.get(_base_url)
    login_action = login_page.html.find('#login_form', first=True).attrs.get('action')
    _session.post(_base_url + login_action, data={'email': email, 'pass': password})
    if 'c_user' not in _session.cookies:
        warnings.warn('login unsuccessful')


def fetch_share_and_reactions(post: dict):
    """Fetch share and reactions information with a existing post obtained by `get_posts`.
    Return a merged post that has some new fields including `reactions`, `w3_fb_url`, `fetched_time`,
        and reactions fields `LIKE`, `ANGER`, `SORRY`, `WOW`, `LOVE`, `HAHA` if exist.

    Note that this method will raise one http request per post, use it when you want some more information.

    Example:
    ```
    for post in get_posts('fanpage'):
        more_info_post = fetch_share_and_reactions(post)
        print(more_info_post)
    ```
    """
    url = post.get('post_url')
    if url:
        w3_fb_url = urlparse.urlparse(url)._replace(netloc='www.facebook.com').geturl()
        resp = _session.get(w3_fb_url, timeout=_timeout)
        for item in _parse_share_and_reactions(resp.text):
            data = (item['jsmods']['pre_display_requires'][0][3][1]['__bbox']['result']
                    ['data']['feedback'])
            if data['subscription_target_id'] == post['post_id']:
                return {
                    **post,
                    'shares': data['share_count']['count'],
                    'likes': data['reactors']['count'],
                    'reactions': {
                        reaction['node']['reaction_type'].lower(): reaction['reaction_count']
                        for reaction in data['top_reactions']['edges']
                    },
                    'comments': data['comment_count']['total_count'],
                    'w3_fb_url': data['url'],
                    'fetched_time': datetime.now(),
                }
    return post


def _parse_share_and_reactions(html: str):
    bad_jsons = _shares_and_reactions_regex.findall(html)
    for bad_json in bad_jsons:
        good_json = _bad_json_key_regex.sub(r'\g<prefix>"\g<key>":', bad_json)
        yield json.loads(good_json)
