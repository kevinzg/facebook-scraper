import itertools
import json
import logging
import re
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Dict, Optional

from . import utils
from .constants import FB_MOBILE_BASE_URL
from .fb_types import RawPost, Options, Post, RequestFunction

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
    link_regex = re.compile(r"href=\"https:\/\/lm\.facebook\.com\/l\.php\?u=(.+?)\&amp;h=")

    photo_link = re.compile(r'href=\"(/[^\"]+/photos/[^\"]+?)\"')
    image_regex = re.compile(
        r'<a href=\"([^\"]+?)\" target=\"_blank\" class=\"sec\">View Full Size<\/a>',
        re.IGNORECASE,
    )
    image_regex_lq = re.compile(r"background-image: url\('(.+)'\)")
    post_url_regex = re.compile(r'/story.php\?story_fbid=')

    shares_and_reactions_regex = re.compile(
        r'<script>.*bigPipe.onPageletArrive\((?P<data>\{.*RelayPrefetchedStreamCache.*\})\);'
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
            'likes': None,
            'comments': None,
            'shares': None,
            'post_url': None,
            'link': None,
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
            self.extract_video,
        ]

        post = self.make_new_post()

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

        if 'reactions' in self.options:
            reactions = self.extract_reactions()
            post.update(reactions)

        return post

    def extract_post_id(self) -> PartialPost:
        return {'post_id': self.data_ft.get('mf_story_key')}

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

        nodes = element.find('p, header')
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

            text = '\n'.join(itertools.chain(post_text, shared_text))
            post_text = '\n'.join(post_text)
            shared_text = '\n'.join(shared_text)

            return {
                'text': text,
                'post_text': post_text,
                'shared_text': shared_text,
            }

        return None

    # TODO: Add the correct timezone
    def extract_time(self) -> PartialPost:
        page_insights = self.data_ft['page_insights']

        for page in page_insights.values():
            try:
                timestamp = page['post_context']['publish_time']
                return {
                    'time': datetime.fromtimestamp(timestamp),
                }
            except (KeyError, ValueError):
                continue

        return None

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

        elements = self.element.find('header a')
        for element in elements:
            href = element.attrs.get('href', '')
            match = self.post_url_regex.match(href)
            if match:
                path = utils.filter_query_params(href, whitelist=query_params)
                url = utils.urljoin(FB_MOBILE_BASE_URL, path)
                return {'post_url': url}
        return None

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
        match = self.photo_link.search(self.element.html)
        if not match:
            return None

        url = utils.urljoin(FB_MOBILE_BASE_URL, match.groups()[0])

        response = self.request(url)
        html = response.text
        match = self.image_regex.search(html)
        if match:
            return {
                'image': match.groups()[0].replace("&amp;", "&"),
            }
        return None

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
        if 'youtube_dl' in self.options:
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
        try:
            post_id = self.post.get('post_id')
            video_page = 'https://www.facebook.com/' + post_id
            with YoutubeDL(ydl_opts) as ydl:
                url = ydl.extract_info(video_page, download=False)['url']
                return {'video': url}
        except ExtractorError as ex:
            logger.error("Error extracting video with youtube-dl: %r", ex)
        return None

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

    # TODO: This might need to be aware of the timezone and locale (?)
    def extract_time(self) -> PartialPost:
        # This is a string with this format 'April 3, 2018 at 8:02 PM'
        time = self.element.find('abbr', first=True).text
        time = datetime.strptime(time, '%B %d, %Y at %I:%M %p')
        return {
            'time': time,
        }
