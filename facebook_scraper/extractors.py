import itertools
import json
import logging
import re
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Dict, Optional

from . import utils
from .constants import FB_BASE_URL, FB_MOBILE_BASE_URL
from .fb_types import Options, Post, RawPost, RequestFunction


try:
    from youtube_dl import YoutubeDL
    from youtube_dl.utils import ExtractorError
except ImportError:
    YoutubeDL = None


logger = logging.getLogger(__name__)

# Typing
PartialPost = Optional[Dict[str, Any]]


def extract_post(raw_post: RawPost, options: Options, request_fn: RequestFunction) -> Post:
    return PostExtractor(raw_post, options, request_fn).extract_post()


def extract_group_post(raw_post: RawPost, options: Options, request_fn: RequestFunction) -> Post:
    return GroupPostExtractor(raw_post, options, request_fn).extract_post()


class PostExtractor:
    """Class for Extracting fields from a FacebookPost"""

    likes_regex = re.compile(r'like_def[^>]*>([0-9,.]+)')
    comments_regex = re.compile(r'cmt_def[^>]*>([0-9,.]+)')
    shares_regex = re.compile(r'([0-9,.]+)\s+Shares', re.IGNORECASE)
    live_regex = re.compile(r'.+(is live).+')
    link_regex = re.compile(r"href=\"https:\/\/lm\.facebook\.com\/l\.php\?u=(.+?)\&amp;h=")

    photo_link = re.compile(r'href=\"(/[^\"]+/photos/[^\"]+?)\"')
    image_regex = re.compile(
        r'<a href=\"([^\"]+?)\" target=\"_blank\" class=\"sec\">View Full Size<\/a>',
        re.IGNORECASE,
    )
    image_regex_lq = re.compile(r"background-image: url\('(.+)'\)")
    video_thumbnail_regex = re.compile(r"background: url\('(.+)'\)")
    post_url_regex = re.compile(r'/story.php\?story_fbid=')
    video_post_url_regex = re.compile(r'/.+/videos/.+/(.+)/.+')
    video_id_regex = re.compile(r'{&quot;videoID&quot;:&quot;([0-9]+)&quot;')

    shares_and_reactions_regex = re.compile(
        r'<script nonce=.*>.*bigPipe.onPageletArrive\((?P<data>\{.*RelayPrefetchedStreamCache.*\})\);'
        '.*</script>'
    )
    bad_json_key_regex = re.compile(r'(?P<prefix>[{,])(?P<key>\w+):')

    more_url_regex = re.compile(r'(?<=…\s)<a href="([^"]+)')
    post_story_regex = re.compile(r'href="(\/story[^"]+)" aria')

    def __init__(self, element, options, request_fn):
        self.element = element
        self.options = options
        self.request = request_fn

        self._data_ft = None

    def make_new_post(self) -> Post:
        return {
            'post_id': None,
            'text': None,
            'post_text': None,
            'shared_text': None,
            'time': None,
            'image': None,
            'video': None,
            'video_thumbnail': None,
            'video_id': None,
            'likes': None,
            'comments': None,
            'shares': None,
            'post_url': None,
            'link': None,
            'user_id': None,
            'username': None,
            'source': None,
            'is_live': False,
        }

    def extract_post(self) -> Post:
        """Parses the element into self.item"""

        methods = [
            self.extract_post_id,
            self.extract_text,
            self.extract_time,
            self.extract_image,
            self.extract_likes,
            self.extract_comments,
            self.extract_shares,
            self.extract_post_url,
            self.extract_link,
            self.extract_user_id,
            self.extract_username,
            self.extract_video,
            self.extract_video_thumbnail,
            self.extract_video_id,
            self.extract_is_live,
        ]

        post = self.make_new_post()
        post['source'] = self.element

        # TODO: this is just used by `extract_reactions`, probably should not be acceded from self
        self.post = post

        for method in methods:
            try:
                partial_post = method()
                if partial_post is None:
                    logger.warning("Extract method %s didn't return anything", method.__name__)
                    continue

                post.update(partial_post)
            except Exception as ex:
                logger.warning("Exception while running %s: %r", method.__name__, ex)

        if self.options.get('reactions'):
            try:
                reactions = self.extract_reactions()
            except Exception as ex:
                logger.warning("Exception while extracting reactions: %r", ex)
                reactions = {}

            if reactions is None:
                logger.warning("Extract reactions didn't return anything")
            else:
                post.update(reactions)

        return post

    def extract_post_id(self) -> PartialPost:
        return {'post_id': self.data_ft.get('mf_story_key')}

    def extract_username(self) -> PartialPost:
        username = self.element.find('h3 strong a')
        return {'username': username[0].text} if len(username) > 0 else None

    # TODO: this method needs test for the 'has more' case and shared content
    def extract_text(self) -> PartialPost:
        # Open this article individually because not all content is fully loaded when skimming
        # through pages.
        # This ensures the full content can be read.

        element = self.element

        has_more = self.more_url_regex.search(element.html)
        if has_more:
            match = self.post_story_regex.search(element.html)
            if match:
                url = utils.urljoin(FB_MOBILE_BASE_URL, match.groups()[0].replace("&amp;", "&"))
                response = self.request(url)
                element = response.html.find('.story_body_container', first=True)

        nodes = element.find('p, header, span[role=presentation]')
        if nodes:
            post_text = []
            shared_text = []
            ended = False
            for node in nodes[1:]:
                if node.tag == 'header':
                    ended = True

                # Remove '... More'
                # This button is meant to display the hidden text that is already loaded
                # Not to be confused with the 'More' that opens the article in a new page
                if node.tag == 'p':
                    node = utils.make_html_element(
                        html=node.html.replace('>… <', '><', 1).replace('>More<', '', 1)
                    )

                if not ended:
                    post_text.append(node.text)
                else:
                    shared_text.append(node.text)

            # Separation between paragraphs
            paragraph_separator = '\n\n'

            text = paragraph_separator.join(itertools.chain(post_text, shared_text))
            post_text = paragraph_separator.join(post_text)
            shared_text = paragraph_separator.join(shared_text)

            return {
                'text': text,
                'post_text': post_text,
                'shared_text': shared_text,
            }

        return None

    # TODO: Add the correct timezone
    def extract_time(self) -> PartialPost:
        # Try to extract time for timestamp
        page_insights = self.data_ft.get('page_insights', {})

        for page in page_insights.values():
            try:
                timestamp = page['post_context']['publish_time']
                return {
                    'time': datetime.fromtimestamp(timestamp),
                }
            except (KeyError, ValueError):
                continue

        # Try to extract from the abbr element
        date_element = self.element.find('abbr', first=True)
        if date_element is not None:
            date = utils.parse_datetime(date_element.text, search=False)
            if date:
                return {'time': date}
            logger.debug("Could not parse date: %s", date_element.text)
        else:
            logger.warning("Could not find the abbr element for the date")

        # Try to look in the entire text
        date = utils.parse_datetime(self.element.text)
        if date:
            return {'time': date}

        return None

    def extract_user_id(self) -> PartialPost:
        return {'user_id': self.data_ft['content_owner_id_new']}

    def extract_image(self) -> PartialPost:
        image_link = self.extract_photo_link()
        if image_link is not None:
            return image_link
        return self.extract_image_lq()

    def extract_image_lq(self) -> PartialPost:
        story_container = self.element.find('div.story_body_container', first=True)
        if story_container is None:
            return None
        other_containers = story_container.xpath('div/div')

        for container in other_containers:
            image_container = container.find('.img', first=True)
            if image_container is None:
                continue

            style = image_container.attrs.get('style', '')
            match = self.image_regex_lq.search(style)
            if match:
                return {'image': utils.decode_css_url(match.groups()[0])}

        return None

    def extract_link(self) -> PartialPost:
        match = self.link_regex.search(self.element.html)
        if match:
            return {'link': utils.unquote(match.groups()[0])}
        return None

    def extract_post_url(self) -> PartialPost:

        query_params = ('story_fbid', 'id')
        account = self.options.get('account')
        elements = self.element.find('header a')
        video_post_match = None
        path = None

        for element in elements:
            href = element.attrs.get('href', '')

            post_match = self.post_url_regex.match(href)
            video_post_match = self.video_post_url_regex.match(href)

            if post_match:
                path = utils.filter_query_params(href, whitelist=query_params)

            elif video_post_match:
                video_post_id = video_post_match.group(1)

                if account is None:
                    path = f'watch?v={video_post_id}'
                else:
                    path = f'{account}/videos/{video_post_id}'

        post_id = self._data_ft.get('top_level_post_id')

        if video_post_match is None and account is not None and post_id is not None:
            path = f'{account}/posts/{post_id}'

        if path is None:
            return None

        url = utils.urljoin(FB_BASE_URL, path)
        return {'post_url': url}

    # TODO: Remove `or 0` from this methods
    def extract_likes(self) -> PartialPost:
        return {
            'likes': utils.find_and_search(
                self.element, 'footer', self.likes_regex, utils.parse_int
            )
            or 0,
        }

    def extract_comments(self) -> PartialPost:
        return {
            'comments': utils.find_and_search(
                self.element, 'footer', self.comments_regex, utils.parse_int
            )
            or 0,
        }

    def extract_shares(self) -> PartialPost:
        return {
            'shares': utils.find_and_search(
                self.element, 'footer', self.shares_regex, utils.parse_int
            )
            or 0,
        }

    def extract_photo_link(self) -> PartialPost:
        images = []
        matches = self.photo_link.finditer(self.element.html)

        for match in matches:
            url = utils.urljoin(FB_MOBILE_BASE_URL, match.groups()[0])

            response = self.request(url)
            html = response.text
            match = self.image_regex.search(html)
            if match:
                images.append(match.groups()[0].replace("&amp;", "&"))
        image = images[0] if images else None
        return {"image": image, "images": images}

    def extract_reactions(self) -> PartialPost:
        """Fetch share and reactions information with a existing post obtained by `get_posts`.
        Return a merged post that has some new fields including `reactions`, `w3_fb_url`,
        `fetched_time`, and reactions fields `LIKE`, `ANGER`, `SORRY`, `WOW`, `LOVE`, `HAHA` if
        exist.
        Note that this method will raise one http request per post, use it when you want some more
        information.

        Example:
        ```
        for post in get_posts('fanpage'):
            more_info_post = fetch_share_and_reactions(post)
            print(more_info_post)
        ```
        """
        url = self.post.get('post_url')
        post_id = self.post.get('post_id')

        if url:
            w3_fb_url = utils.urlparse(url)._replace(netloc='www.facebook.com').geturl()
            resp = self.request(w3_fb_url)
            for item in self.parse_share_and_reactions(resp.text):
                data = item['jsmods']['pre_display_requires'][0][3][1]['__bbox']['result'][
                    'data'
                ]['feedback']
                if data['subscription_target_id'] == post_id:
                    return {
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
        return None

    def extract_video(self):
        video_data_element = self.element.find('[data-sigil="inlineVideo"]', first=True)
        if video_data_element is None:
            return None
        if self.options.get('youtube_dl'):
            vid = self.extract_video_highres()
            if vid:
                return vid
        return self.extract_video_lowres(video_data_element)

    def extract_video_lowres(self, video_data_element):
        try:
            data = json.loads(video_data_element.attrs['data-store'])
            return {'video': data.get('src')}
        except JSONDecodeError as ex:
            logger.error("Error parsing data-store JSON: %r", ex)
        except KeyError:
            logger.error("data-store attribute not found")
        return None

    def extract_video_highres(self):
        if not YoutubeDL:
            raise ModuleNotFoundError(
                "youtube-dl must be installed to download videos in high resolution."
            )

        ydl_opts = {
            'format': 'best',
            'quiet': True,
        }
        if self.options.get('youtube_dl_verbose'):
            ydl_opts['quiet'] = False

        try:
            post_id = self.post.get('post_id')
            if post_id is None:
                return None

            video_page = 'https://www.facebook.com/' + post_id
            with YoutubeDL(ydl_opts) as ydl:
                url = ydl.extract_info(video_page, download=False)['url']
                return {'video': url}
        except ExtractorError as ex:
            logger.error("Error extracting video with youtube-dl: %r", ex)

        return None

    def extract_video_thumbnail(self):
        thumbnail_element = self.element.find('i[data-sigil="playInlineVideo"]', first=True)
        if not thumbnail_element:
            return None
        style = thumbnail_element.attrs.get('style', '')
        match = self.video_thumbnail_regex.search(style)
        if match:
            return {'video_thumbnail': utils.decode_css_url(match.groups()[0])}
        return None

    def extract_video_id(self):
        match = self.video_id_regex.search(self.element.html)
        if match:
            return {'video_id': match.groups()[0]}
        return None

    def extract_is_live(self):
        header = self.element.find('header')[0].full_text

        match = self.live_regex.search(header)

        if match is not None:
            return {'is_live': True}

        return {'is_live': False}

    def parse_share_and_reactions(self, html: str):
        bad_jsons = self.shares_and_reactions_regex.findall(html)
        for bad_json in bad_jsons:
            good_json = self.bad_json_key_regex.sub(r'\g<prefix>"\g<key>":', bad_json)
            yield json.loads(good_json)

    @property
    def data_ft(self) -> dict:
        if self._data_ft is not None:
            return self._data_ft

        self._data_ft = {}
        try:
            data_ft_json = self.element.attrs['data-ft']
            self._data_ft = json.loads(data_ft_json)
        except JSONDecodeError as ex:
            logger.error("Error parsing data-ft JSON: %r", ex)
        except KeyError:
            logger.error("data-ft attribute not found")

        return self._data_ft


class GroupPostExtractor(PostExtractor):
    """Class for extracting posts from Facebook Groups rather than Pages"""
