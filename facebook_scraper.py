from datetime import datetime
import json
import re

from requests_html import HTMLSession

__all__ = ['get_posts']


_likes_regex = re.compile(r'([0-9,.]+)\s+Like')
_comments_regex = re.compile(r'([0-9,.]+)\s+Comment')
_shares_regex = re.compile(r'([0-9,.]+)\s+Shares')


def get_posts(account):
    url = f'https://m.facebook.com/{account}/posts/'

    session = HTMLSession()
    session.headers.update({
        'Accept-Language': 'en-US,en;q=0.5',
    })

    response = session.get(url)
    html = response.html

    for article in html.find('article'):
        yield _extract_post(article)


def _extract_post(article):
    return {
        'post_id': _extract_post_id(article),
        'text': _extract_text(article),
        'time': _extract_time(article),
        'likes': _find_and_search(article, 'footer', _likes_regex, _parse_int) or 0,
        'comments': _find_and_search(article, 'footer', _comments_regex, _parse_int) or 0,
        'shares':  _find_and_search(article, 'footer', _shares_regex, _parse_int) or 0,
    }


def _extract_post_id(article):
    data_ft = json.loads(article.attrs['data-ft'])
    return data_ft['mf_story_key']


def _extract_text(article):
    paragraph = article.find('p', first=True)
    return paragraph and paragraph.text


def _extract_time(article):
    data_ft = json.loads(article.attrs['data-ft'])
    page_insights = data_ft['page_insights']
    for page in page_insights.values():
        try:
            timestamp = page['post_context']['publish_time']
            return datetime.fromtimestamp(timestamp)
        except (KeyError, ValueError):
            continue
    return None


def _find_and_search(article, selector, pattern, cast=str):
    container = article.find(selector, first=True)
    text = container and container.text
    match = text and pattern.search(text)
    return match and cast(match.group())


def _parse_int(value):
    return int(''.join(filter(lambda c: c.isdigit(), value)))
