import itertools
import json
import demjson3 as demjson
from demjson3 import JSONDecodeError
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse
from tqdm.auto import tqdm

from . import utils, exceptions
from .constants import FB_BASE_URL, FB_MOBILE_BASE_URL, FB_W3_BASE_URL
from .fb_types import Options, Post, RawPost, RequestFunction, Response, URL


try:
    from youtube_dl import YoutubeDL
    from youtube_dl.utils import ExtractorError
except ImportError:
    YoutubeDL = None


logger = logging.getLogger(__name__)

# Typing
PartialPost = Optional[Dict[str, Any]]


def extract_post(
    raw_post: RawPost, options: Options, request_fn: RequestFunction, full_post_html=None
) -> Post:
    return PostExtractor(raw_post, options, request_fn, full_post_html).extract_post()


def extract_group_post(
    raw_post: RawPost, options: Options, request_fn: RequestFunction, full_post_html=None
) -> Post:
    return GroupPostExtractor(raw_post, options, request_fn, full_post_html).extract_post()


def extract_story_post(
    raw_post: RawPost, options: Options, request_fn: RequestFunction, full_post_html=None
) -> Post:
    return StoryExtractor(raw_post, options, request_fn, full_post_html).extract_post()


def extract_photo_post(
    raw_post: RawPost, options: Options, request_fn: RequestFunction, full_post_html
) -> Post:
    return PhotoPostExtractor(raw_post, options, request_fn, full_post_html).extract_post()


def extract_hashtag_post(
    raw_post: RawPost, options: Options, request_fn: RequestFunction, full_post_html=None
) -> Post:
    return HashtagPostExtractor(raw_post, options, request_fn, full_post_html).extract_post()


class PostExtractor:
    """Class for Extracting fields from a FacebookPost"""

    likes_regex = re.compile(
        r'([\d,.KM]+)\s+(Like|left reaction|others reacted|others left reactions)', re.IGNORECASE
    )
    comments_regex = re.compile(r'([\d,.KM]+)\s+comment', re.IGNORECASE)
    shares_regex = re.compile(r'([\d,.KM]+)\s+Share', re.IGNORECASE)
    link_regex = re.compile(r"href=\"https:\/\/lm\.facebook\.com\/l\.php\?u=(.+?)\&amp;h=")

    photo_link = re.compile(r'href=\"(/[^\"]+/photos/[^\"]+?)\"')
    photo_link_2 = re.compile(r'href=\"(/photo.php[^\"]+?)\"')
    image_regex = re.compile(
        r'<a href=\"([^\"]+?)\" target=\"_blank\" class=\"sec\">',
        re.IGNORECASE,
    )
    image_regex_lq = re.compile(r"url\('(.+)'\)")
    video_thumbnail_regex = re.compile(r"background: url\('(.+)'\)")
    post_url_regex = re.compile(r'/story.php\?story_fbid=')
    video_post_url_regex = re.compile(r'/.+/videos/.+/(.+)/.+')
    video_id_regex = re.compile(r'{&quot;videoID&quot;:&quot;([0-9]+)&quot;')

    shares_and_reactions_regex = re.compile(
        r'<script nonce=.*>.*bigPipe.onPageletArrive\((?P<data>\{.*RelayPrefetchedStreamCache.*\})\);'
        '.*</script>'
    )
    bad_json_key_regex = re.compile(r'(?P<prefix>[{,])(?P<key>\w+):')

    more_url_regex = re.compile(r'(?<=…\s)<a')
    post_story_regex = re.compile(r'href="(\/story[^"]+)" aria')

    def __init__(self, element, options, request_fn, full_post_html=None):
        self.element = element
        self.options = options
        self.request = request_fn

        self._data_ft = None
        self._full_post_html = full_post_html
        self._live_data = {}

    # TODO: This is getting ugly, create a dataclass for Post
    def make_new_post(self) -> Post:
        return {
            'post_id': None,
            'text': None,
            'post_text': None,
            'shared_text': None,
            'original_text': None,
            'time': None,
            'timestamp': None,
            'image': None,
            'image_lowquality': None,
            'images': None,
            'images_description': None,
            'images_lowquality': None,
            'images_lowquality_description': None,
            'video': None,
            'video_duration_seconds': None,
            'video_height': None,
            'video_id': None,
            'video_quality': None,
            'video_size_MB': None,
            'video_thumbnail': None,
            'video_watches': None,
            'video_width': None,
            'likes': None,
            'comments': None,
            'shares': None,
            'post_url': None,
            'link': None,
            'links': None,
            'user_id': None,
            'username': None,
            'user_url': None,
            'source': None,
            'is_live': False,
            'factcheck': None,
            'shared_post_id': None,
            'shared_time': None,
            'shared_user_id': None,
            'shared_username': None,
            'shared_post_url': None,
            'available': None,
            'comments_full': None,
            'reactors': None,
            'w3_fb_url': None,
            'reactions': None,
            'reaction_count': None,
            'with': None,
            'page_id': None,
            'sharers': None,
        }

    def extract_post(self) -> Post:
        """Parses the element into self.item"""

        methods = [
            self.extract_post_url,
            self.extract_post_id,
            self.extract_text,
            self.extract_time,
            self.extract_photo_link,
            self.extract_image_lq,
            self.extract_likes,
            self.extract_comments,
            self.extract_shares,
            self.extract_links,
            self.extract_user_id,
            self.extract_username,
            self.extract_video,
            self.extract_video_thumbnail,
            self.extract_video_id,
            self.extract_video_meta,
            self.extract_is_live,
            self.extract_factcheck,
            self.extract_share_information,
            self.extract_availability,
            self.extract_listing,
            self.extract_with,
        ]

        post = self.make_new_post()
        post['source'] = self.element

        # TODO: this is just used by `extract_reactions`, probably should not be acceded from self
        self.post = post

        def log_warning(msg, *args):
            post_id = self.post.get('post_id', 'unknown post')
            logger.warning(f"[%s] {msg}", post_id, *args)

        for method in methods:
            try:
                partial_post = method()
                if partial_post is None:
                    log_warning("Extract method %s didn't return anything", method.__name__)
                    continue

                post.update(partial_post)
            except exceptions.TemporarilyBanned:
                raise
            except Exception as ex:
                log_warning("Exception while running %s: %r", method.__name__, ex)

        has_more = self.more_url_regex.search(self.element.html)
        if has_more and self.full_post_html:
            post['source'] = self.full_post_html.find('.story_body_container', first=True)

        if self.options.get('reactions') or self.options.get('reactors'):
            try:
                reactions = self.extract_reactions()
                if reactions["reactors"] and self.options.get("reactors") != "generator":
                    # Consume reactor generator to return list
                    reactions["reactors"] = utils.safe_consume(reactions["reactors"])
            except Exception as ex:
                log_warning("Exception while extracting reactions: %r", ex)
                reactions = {}

            if reactions is None:
                log_warning("Extract reactions didn't return anything")
            else:
                post.update(reactions)

        if self.options.get("sharers"):
            try:
                post["sharers"] = self.extract_sharers()
                if self.options.get("sharers") != "generator":
                    post["sharers"] = utils.safe_consume(post["sharers"])
            except Exception as ex:
                log_warning("Exception while extracting sharers: %r", ex)

        if self.options.get('comments'):
            try:
                post["comments_full"] = self.extract_comments_full()
                if self.options.get("comments") != "generator":
                    # Consume both comment generator and reply generator to return lists
                    post["comments_full"] = utils.safe_consume(post["comments_full"])
                    for comment in post["comments_full"]:
                        comment["replies"] = utils.safe_consume(comment["replies"])
                        for reply in comment["replies"]:
                            utils.safe_consume(reply["comment_reactors"])
                        comment["comment_reactors"] = utils.safe_consume(
                            comment["comment_reactors"]
                        )
                    if post.get("comments_full") and not post.get("comments"):
                        post["comments"] = len(post.get("comments_full"))

            except Exception as ex:
                log_warning("Exception while extracting comments: %r", ex)
        return post

    def extract_post_id(self) -> PartialPost:
        return {
            'post_id': self.live_data.get("ft_ent_identifier")
            or self.data_ft.get('top_level_post_id')
        }

    def extract_username(self) -> PartialPost:
        elem = self.element.find('h3 strong a,a.actor-link', first=True)
        if elem:
            url = elem.attrs.get("href")
            if url:
                url = utils.urljoin(FB_BASE_URL, url)
            return {'username': elem.text, 'user_url': url}

    # TODO: this method needs test for the 'has more' case and shared content
    def extract_text(self) -> PartialPost:
        # Open this article individually because not all content is fully loaded when skimming
        # through pages.
        # This ensures the full content can be read.

        element = self.element

        has_more = self.more_url_regex.search(element.html)
        if has_more and self.full_post_html:
            element = self.full_post_html.find('.story_body_container', first=True)

        nodes = element.find('p, header, span[role=presentation]')
        if nodes and len(nodes) > 1:
            post_text = []
            shared_text = []
            ended = False
            index_non_header = next(
                (i for i, node in enumerate(nodes) if node.tag != 'header'), 1
            )
            for node in nodes[index_non_header:]:
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

            original_text = None
            hidden_div = element.find('div[style="display:none"]', first=True)
            if hidden_div:
                original_text = []
                for node in hidden_div.find("p,span[role=presentation]"):
                    node = utils.make_html_element(
                        html=node.html.replace('>… <', '><', 1).replace('>More<', '', 1)
                    )
                    original_text.append(node.text)
                original_text = paragraph_separator.join(original_text)

            return {
                'text': text,
                'post_text': post_text,
                'shared_text': shared_text,
                'original_text': original_text,
            }
        elif element.find(".story_body_container>div", first=True):
            text = element.find(".story_body_container>div", first=True).text
            return {'text': text, 'post_text': text}
        elif len(nodes) == 1:
            text = nodes[0].text
            return {'text': text, 'post_text': text}

        return None

    # TODO: Add the correct timezone
    def extract_time(self) -> PartialPost:
        # Try to extract time for timestamp
        page_insights = self.data_ft.get('page_insights', {})

        for page in page_insights.values():
            try:
                timestamp = page['post_context']['publish_time']
                logger.debug(
                    f"Got exact timestamp from publish_time: {datetime.fromtimestamp(timestamp)}"
                )
                return {'time': datetime.fromtimestamp(timestamp), 'timestamp': timestamp}
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

        try:
            date_element = self.full_post_html.find("abbr[data-store*='time']", first=True)
            time = json.loads(date_element.attrs["data-store"])["time"]
            logger.debug(
                f"Got exact timestamp from abbr[data-store]: {datetime.fromtimestamp(time)}"
            )
            return {'time': datetime.fromtimestamp(time), 'timestamp': time}
        except:
            return None

    def extract_user_id(self) -> PartialPost:
        return {
            'user_id': self.data_ft['content_owner_id_new'],
            'page_id': self.data_ft.get("page_id"),
        }

    def extract_image_lq(self) -> PartialPost:
        elems = self.element.find('div.story_body_container>div .img:not(.profpic)')
        if not elems:
            elems = self.element.find('.img:not(.profpic), img:not(.profpic)')
        images = []
        image_ids = []
        descriptions = []
        for elem in elems:
            url = None
            if elem.attrs.get('src'):
                url = elem.attrs.get('src')
            elif elem.attrs.get('style'):
                match = self.image_regex_lq.search(elem.attrs.get('style'))
                if match:
                    url = utils.decode_css_url(match.groups()[0])
            if url and "static.xx.fbcdn.net" not in url:
                images.append(url)
                descriptions.append(elem.attrs.get("aria-label") or elem.attrs.get("alt"))
            url = elem.element.getparent().getparent().getparent().attrib.get("href")
            if url:
                image_ids.append(re.search(r'[=/](\d+)', url).group(1))

        image = images[0] if images else None
        image_id = image_ids[0] if image_ids else None
        result = {
            "image_lowquality": image,
            "image_id": image_id,
            "image_ids": image_ids,
            "images_lowquality": images,
            "images_lowquality_description": descriptions,
        }
        # Link to high resolution external image embedded in low quality image url
        if image and "safe_image.php" in image and not self.post.get("image"):
            url = parse_qs(urlparse(image).query).get("url")
            if url:
                url = url[0]
                result.update({"image": url, "images": [url]})
        return result

    def extract_links(self) -> PartialPost:
        link = self.link_regex.search(self.element.html)
        if link:
            link = utils.unquote(link.groups()[0])
        links = self.element.find(".story_body_container div p a")
        links = [{"link": a.attrs["href"], "text": a.text} for a in links]
        return {"link": link, "links": links}

    def extract_post_url(self) -> PartialPost:

        query_params = ('story_fbid', 'id')
        account = self.options.get('account')
        elements = self.element.find('a')
        video_post_match = None
        path = None

        for element in elements:
            href = element.attrs.get('href', '')

            post_match = self.post_url_regex.match(href)
            video_post_match = self.video_post_url_regex.match(href)

            if post_match:
                path = utils.filter_query_params(href, whitelist=query_params)
                break

            elif video_post_match:
                video_post_id = video_post_match.group(1)

                if account is None:
                    path = f'watch?v={video_post_id}'
                else:
                    path = f'{account}/videos/{video_post_id}'

        post_id = self.data_ft.get('top_level_post_id')

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
                self.element, 'footer', self.likes_regex, utils.convert_numeric_abbr
            )
            or self.live_data.get("like_count")
            or self.live_data.get("reactioncount")
            or (
                self.element.find(".likes", first=True)
                and utils.parse_int(self.element.find(".likes", first=True).text)
            )
            or (
                self.element.find(".like_def", first=True)
                and utils.parse_int(self.element.find(".like_def", first=True).text)
            )
            or 0,
        }

    def extract_comments(self) -> PartialPost:
        return {
            'comments': utils.find_and_search(
                self.element, 'footer', self.comments_regex, utils.convert_numeric_abbr
            )
            or self.live_data.get("comment_count")
            or (
                self.element.find(".cmt_def", first=True)
                and utils.parse_int(self.element.find(".cmt_def", first=True).text)
            )
            or 0,
        }

    def extract_shares(self) -> PartialPost:
        return {
            'shares': utils.find_and_search(
                self.element, 'footer', self.shares_regex, utils.convert_numeric_abbr
            )
            or self.live_data.get("share_count")
            or 0,
        }

    def extract_photo_link_HQ(self, html: str) -> URL:
        # Find a link that says "View Full Size"
        match = self.image_regex.search(html)
        if match:
            url = match.groups()[0].replace("&amp;", "&")
            if not url.startswith("http"):
                url = utils.urljoin(FB_MOBILE_BASE_URL, url)
            if url.startswith(utils.urljoin(FB_MOBILE_BASE_URL, "/photo/view_full_size/")):
                # Try resolve redirect
                logger.debug(f"Fetching {url}")
                try:
                    redirect_response = self.request(url)
                    url = (
                        redirect_response.html.find("a", first=True)
                        .attrs.get("href")
                        .replace("&amp;", "&")
                    )
                except Exception as e:
                    logger.error(e)
            return url
        else:
            return None

    def extract_photo_link(self) -> PartialPost:
        if not self.options.get("allow_extra_requests", True) or not self.options.get(
            "HQ_images", True
        ):
            return None
        images = []
        descriptions = []
        image_ids = []
        raw_photo_links = self.element.find(
            "div.story_body_container>div a[href*='photo.php'], "
            "div.story_body_container>div a[href*='/photos/'], "
            "div._5v64 a[href*='/photos/']"
        )
        photo_links = []
        seen_urls = []
        for a in raw_photo_links:
            partial_url = a.attrs["href"].split("?")[0]
            if partial_url not in seen_urls:
                photo_links.append(a)
                seen_urls.append(partial_url)
        total_photos_in_gallery = len(photo_links)
        if len(photo_links) in [4, 5] and photo_links[-1].text:
            total_photos_in_gallery = len(photo_links) + int(photo_links[-1].text.strip("+")) - 1
            logger.debug(f"{total_photos_in_gallery} total photos in gallery")

        # This gets up to 4 images in gallery
        for link in photo_links:
            url = link.attrs["href"]
            if "photoset_token" in url:
                query = parse_qs(urlparse(url).query)
                profile_id = query["profileid"][0]
                token = query["photoset_token"][0]
                url = f"{profile_id}/posts/{token}"
                logger.debug(f"Fetching {url}")
                response = self.request(url)
                results = self.get_jsmod("mtouch_snowflake_paged_query", response.html)
                results = list(results["query_results"].values())[0]["media"]
                video_ids = []
                videos = []
                for item in results["edges"]:
                    node = item["node"]
                    if node["is_playable"]:
                        video_ids.append(node["id"])
                        videos.append(node["playable_url_hd"] or node["playable_url"])
                        images.append(node["full_width_image"]["uri"])
                        image_ids.append(node["id"])
                    else:
                        url = node["url"]
                        url = url.replace(FB_W3_BASE_URL, FB_MOBILE_BASE_URL)
                        logger.debug(f"Fetching {url}")
                        response = self.request(url)
                        images.append(self.extract_photo_link_HQ(response.text))
                        image_ids.append(node["id"])
                    descriptions.append(node["accessibility_caption"])
                return {
                    "image": images[0] if images else None,
                    "images": images,
                    "images_description": descriptions,
                    "image_id": image_ids[0] if image_ids else None,
                    "image_ids": image_ids,
                    "video": videos[0] if videos else None,
                    "video_id": video_ids[0] if video_ids else None,
                    "video_ids": video_ids,
                    "videos": videos,
                }
            url = utils.urljoin(FB_MOBILE_BASE_URL, url)
            logger.debug(f"Fetching {url}")
            try:
                response = self.request(url)
                images.append(self.extract_photo_link_HQ(response.text))
                elem = response.html.find(".img[data-sigil='photo-image']", first=True)
                descriptions.append(elem.attrs.get("alt") or elem.attrs.get("aria-label"))
                image_ids.append(re.search(r'[=/](\d+)', url).group(1))
            except Exception as e:
                logger.error(e)
                total_photos_in_gallery -= 1

        errors = 0
        while len(images) < total_photos_in_gallery:
            # More photos to fetch. Follow the left arrow link of the last image we were on
            direction = '{"tn":"+>"}'
            if response.html.find("a", containing="Photos from", first=True):
                # Right arrow link
                direction = '{"tn":"+="}'
            url = response.html.find(f"a.touchable[data-gt='{direction}']", first=True).attrs[
                "href"
            ]
            if not url.startswith("http"):
                url = utils.urljoin(FB_MOBILE_BASE_URL, url)
            logger.debug(f"Fetching {url}")
            response = self.request(url)
            photo_link = self.extract_photo_link_HQ(response.text)
            if photo_link not in images:
                images.append(photo_link)
                elem = response.html.find(".img[data-sigil='photo-image']", first=True)
                descriptions.append(elem.attrs.get("alt") or elem.attrs.get("aria-label"))
                image_ids.append(re.search(r'[=/](\d+)', url).group(1))
            else:
                errors += 1
                if errors > 5:
                    logger.error("Reached image error limit")
                    break
        image = images[0] if images else None
        image_id = image_ids[0] if image_ids else None
        return {
            "image": image,
            "images": images,
            "images_description": descriptions,
            "image_id": image_id,
            "image_ids": image_ids,
        }

    def extract_reactors(self, response, reaction_lookup=utils.reaction_lookup):
        """Fetch people reacting to an existing post obtained by `get_posts`.
        Note that this method may raise one more http request per post to get all reactors"""
        emoji_url_lookup = {}
        spriteMapCssClass = "sp_LdwxfpG67Bn"
        emoji_class_lookup = utils.emoji_class_lookup
        reaction_icons = self.get_jsmod("UFIReactionIcons")
        if reaction_icons:
            for k, v in reaction_icons.items():
                name = reaction_lookup[k]["display_name"].lower()
                for item in v.values():
                    emoji_class_lookup[item["spriteCssClass"]] = name
                    spriteMapCssClass = item["spriteMapCssClass"]
        for sigil in response.html.find("span[data-sigil='reaction_profile_sigil']"):
            single_reaction = demjson.decode(sigil.attrs.get("data-store"))
            if "reactionType" in single_reaction:
                k = str(single_reaction["reactionType"])
            else:
                k = str(single_reaction["reactionID"])
            if k == "all":
                continue
            name = reaction_lookup[k]["display_name"].lower()
            emoji_style = sigil.find("i", first=True).attrs.get("style")
            emoji_url = utils.get_background_image_url(emoji_style)
            emoji_url_lookup[emoji_url] = name

        reactors_opt = self.options.get("reactors")
        limit = 1e9
        if type(reactors_opt) in [int, float] and reactors_opt < limit:
            limit = reactors_opt
        logger.debug(f"Fetching {limit} reactors")
        elems = list(response.html.find("div[id^='reaction_profile_browser']>div"))
        for elem in elems:
            try:
                emoji_class = elem.find(f"div>i.{spriteMapCssClass}", first=True).attrs.get(
                    "class"
                )[-1]
                reaction_type = emoji_class_lookup.get(emoji_class)
                if not reaction_type:
                    logger.error(f"Don't know {emoji_class}")
            except AttributeError:
                try:
                    emoji_style = elem.find(f"div>i[style]", first=True).attrs.get("style")
                    emoji_url = utils.get_background_image_url(emoji_style)
                    reaction_type = emoji_url_lookup.get(emoji_url)
                    if not reaction_type:
                        logger.error(f"Don't know {emoji_url}")
                except AttributeError:
                    logger.error(f"No div>i[style] elem in: {elem}")
                    reaction_type = None
            yield {
                "name": elem.find("strong", first=True).text,
                "link": utils.urljoin(FB_BASE_URL, elem.find("a", first=True).attrs.get("href")),
                "type": reaction_type,
            }
        more = response.html.find("div[id^=reaction_profile_pager] a", first=True)
        while more and len(elems) < limit:
            url = utils.urljoin(FB_MOBILE_BASE_URL, more.attrs.get("href"))
            logger.debug(f"Fetching {url}")
            try:
                response = self.request(url)
            except Exception as e:
                logger.error(e)
                break
            prefix_length = len('for (;;);')
            data = json.loads(response.text[prefix_length:])  # Strip 'for (;;);'
            more = None
            for action in data['payload']['actions']:
                if action['cmd'] == 'append':
                    html = utils.make_html_element(
                        f"<div id='reaction_profile_browser'>{action['html']}</div>",
                        url=FB_MOBILE_BASE_URL,
                    )
                    elems = html.find(
                        'div#reaction_profile_browser>div,div#reaction_profile_browser1>div'
                    )
                    for elem in elems:
                        if not elem.find(f"div>i.{spriteMapCssClass}", first=True):
                            # Try update spriteMapCssClass
                            classes = elem.find("div>i.img", first=True).attrs["class"]
                            for c in classes:
                                if c.startswith("sp_"):
                                    spriteMapCssClass = c
                        try:
                            emoji_class = elem.find(
                                f"div>i.{spriteMapCssClass}", first=True
                            ).attrs.get("class")[-1]
                            reaction_type = emoji_class_lookup.get(emoji_class)
                            if not reaction_type:
                                logger.error(f"Don't know {emoji_class}")
                        except AttributeError:
                            try:
                                emoji_style = elem.find(f"div>i[style]", first=True).attrs.get(
                                    "style"
                                )
                                emoji_url = utils.get_background_image_url(emoji_style)
                                reaction_type = emoji_url_lookup.get(emoji_url)
                                if not reaction_type:
                                    logger.error(f"Don't know {emoji_url}")
                            except AttributeError:
                                logger.error(f"No div>i[style] elem in: {elem.html}")
                                reaction_type = None
                        yield {
                            "name": elem.find("strong", first=True).text,
                            "link": utils.urljoin(
                                FB_BASE_URL, elem.find("a", first=True).attrs.get("href")
                            ),
                            "type": reaction_type,
                        }
                elif action['cmd'] == 'replace':
                    html = utils.make_html_element(
                        f"<div id='reaction_profile_browser'>{action['html']}</div>",
                        url=FB_MOBILE_BASE_URL,
                    )
                    more = html.find("div#reaction_profile_pager a", first=True)

    def extract_sharers(self):
        """Fetch people sharing an existing post obtained by `get_posts`.
        Note that this method may raise more http requests per post to get all sharers"""
        share_url = f'https://m.facebook.com/browse/shares?id={self.post.get("post_id")}'
        while share_url:
            logger.debug(f"Fetching {share_url}")
            response = self.request(share_url)
            elems = response.html.find("div.item:not(#m_more_item)")
            for elem in elems:
                yield {
                    "name": elem.find("strong", first=True).text,
                    "link": utils.urljoin(
                        FB_BASE_URL, elem.find("a", first=True).attrs.get("href")
                    ),
                }
            more = response.html.find("#m_more_item a", first=True)
            if more:
                share_url = more.attrs.get("href")
            else:
                share_url = None

    def extract_reactions(self, post_id=None, force_parse_HTML=False) -> PartialPost:
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
        reactions = {}

        reaction_lookup = utils.reaction_lookup
        reaction_lookup_jsmod = self.get_jsmod("UFIReactionTypes")
        if reaction_lookup_jsmod:
            reaction_lookup.update(reaction_lookup_jsmod.get("reactions"))
        for k, v in self.live_data.get("reactioncountmap", {}).items():
            if v["default"]:
                name = reaction_lookup[k]["display_name"].lower()
                reactions[name] = v["default"]
        reaction_count = self.live_data.get("reactioncount")

        url = self.post.get('post_url')
        if not post_id:
            post_id = self.post.get("post_id")
        w3_fb_url = url and utils.urlparse(url)._replace(netloc='www.facebook.com').geturl()

        reactors_opt = self.options.get("reactors")
        reactors = []
        if reactors_opt:
            reaction_url = f'https://m.facebook.com/ufi/reaction/profile/browser/?ft_ent_identifier={post_id}'
            logger.debug(f"Fetching {reaction_url}")
            response = self.request(reaction_url)
            if not reactions or force_parse_HTML:
                reactions = {}
                reaction_count = 0
                for sigil in response.html.find("span[data-sigil='reaction_profile_sigil']"):
                    single_reaction = demjson.decode(sigil.attrs.get("data-store"))
                    if "reactionType" in single_reaction:
                        k = str(single_reaction["reactionType"])
                    else:
                        k = str(single_reaction["reactionID"])
                    v = sigil.find(
                        "span[data-sigil='reaction_profile_tab_count']", first=True
                    ).text.replace("All ", "")
                    v = utils.convert_numeric_abbr(v)
                    if k == "all":
                        reaction_count = v
                    elif k in reaction_lookup:
                        name = reaction_lookup[k]["display_name"].lower()
                        reactions[name] = v
                if not reaction_count:
                    reaction_count = sum(reactions.values())
            reactors = self.extract_reactors(response, reaction_lookup)

        if reactions:
            return {
                'likes': reactions.get("like"),
                'reactions': reactions,
                'reaction_count': reaction_count,
                'reactors': reactors,
                'fetched_time': datetime.now(),
                'w3_fb_url': w3_fb_url,
            }

        if url:
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
        return {'fetched_time': datetime.now()}

    def extract_video(self):
        video_data_element = self.element.find('[data-sigil="inlineVideo"]', first=True)
        photoset_link = self.element.find("a[href*='photoset_token']", first=True)
        if photoset_link and photoset_link.find("i[aria-label='video']"):
            query = parse_qs(urlparse(photoset_link.attrs.get("href")).query)
            video_id = query["photo"][0]
            if video_id != self.post["post_id"]:
                logger.debug(f"Fetching {video_id}")
                response = self.request(video_id)
                video_post = PostExtractor(
                    response.html, self.options, self.request, full_post_html=response.html
                )
                video_post.post = {"post_id": video_id}
                meta = video_post.extract_video_meta() or {}
                return {
                    "video_id": video_id,
                    "video": video_post.extract_video().get("video"),
                    **meta,
                }

        if video_data_element is None:
            return None
        if self.options.get('youtube_dl'):
            vid = self.extract_video_highres()
            if vid:
                return vid
        return self.extract_video_lowres(video_data_element)

    def extract_video_lowres(self, video_data_element):
        try:
            data = demjson.decode(video_data_element.attrs['data-store'].replace("\\\\", "\\"))
            return {'video': data.get('src').replace("\\/", "/")}
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
            with YoutubeDL(ydl_opts) as ydl:
                url = ydl.extract_info(self.post.get("post_url"), download=False)['url']
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

    def extract_video_meta(self):
        elem = self.full_post_html.find("script[type='application/ld+json']", first=True)
        if not elem:
            return None
        meta = json.loads(elem.text)
        if meta.get("@type") != "VideoObject":
            return
        watches = 0
        if "interactionStatistic" in meta:
            for interaction in meta["interactionStatistic"]:
                if interaction.get("interactionType")["@type"] == 'http://schema.org/WatchAction':
                    watches = interaction.get("userInteractionCount")
        contentSize = None
        if meta.get("contentSize"):
            contentSize = float(meta['contentSize'].strip("kB")) / 1000

        time = utils.parse_datetime(meta["datePublished"])
        # Remove the timezone attribute to make it timezone-naive
        time = time.astimezone().replace(tzinfo=None)
        return {
            "time": time,
            'video_duration_seconds': utils.parse_duration(meta.get("duration")),
            'video_watches': watches,
            'video_quality': meta.get('videoQuality'),
            'video_width': meta.get('width'),
            'video_height': meta.get('height'),
            'video_size_MB': contentSize,
        }

    def extract_is_live(self):
        header = self.element.find('header')[0].full_text
        return {'is_live': "is live" in header, 'was_live': "was live" in header}

    def extract_factcheck(self):
        button = self.element.find('button[value="See Why"]', first=True)
        if not button:
            return None
        factcheck_div = button.element.getparent().getparent()
        factcheck = ""
        for text in factcheck_div.itertext():
            if text.strip() == "See Why":
                continue
            factcheck += text + "\n"
        return {'factcheck': factcheck}

    def extract_share_information(self):
        if not self.data_ft.get("original_content_id"):
            return None
        logger.debug(
            "%s is a share of %s", self.post["post_id"], self.data_ft["original_content_id"]
        )
        # A shared post contains an <article> element within it's own <article> element, or a header element for a shared image
        raw_post = self.element.find(
            "article article, .story_body_container .story_body_container header", first=True
        )
        # We can re-use the existing parsers, as a one level deep recursion
        shared_post = PostExtractor(raw_post, self.options, self.request)
        return {
            'shared_post_id': self.data_ft["original_content_id"],
            'shared_time': shared_post.extract_time().get("time"),
            'shared_user_id': self.data_ft["original_content_owner_id"],
            'shared_username': shared_post.extract_username().get("username"),
            'shared_post_url': shared_post.extract_post_url().get("post_url"),
        }

    def extract_availability(self):
        return {
            'available': ">This content isn't available at the moment<" not in self.element.html
        }

    def parse_comment(self, comment):
        comment_id = comment.attrs.get("id")

        try:
            profile_picture = comment.find(".profpic.img", first=True)
            name = profile_picture.attrs.get("alt") or profile_picture.attrs.get("aria-label")
            name = name.split(",")[0]
            commenter_id = re.search(r'feed_story_ring(\d+)', comment.html)
            if commenter_id:
                commenter_id = commenter_id.group(1)

            url = profile_picture.element.getparent().attrib.get("href")
            if url:
                url = utils.urljoin(FB_BASE_URL, url)
        except AttributeError:
            name = comment.find("h3", first=True).text
            commenter_id = None
            url = None
            link = comment.find("h3>a", first=True)
            if link:
                url = utils.urljoin(FB_BASE_URL, link.attrs.get("href"))
        first_link = comment.find(
            "div:not([data-sigil])>a[href]:not([data-click]):not([data-store]):not([data-sigil])",
            first=True,
        )
        comment_body_elem = comment.find(
            '[data-sigil="comment-body"],div._14ye,div.bl', first=True
        )
        if not comment_body_elem:
            comment_body_elem = comment.find('div>div>div', first=True)
        if comment_body_elem:
            text = comment_body_elem.text
        else:
            text = comment.text
        commenter_meta = None
        if first_link:
            if "\n" in first_link.text:
                commenter_meta = first_link.text.split("\n")[0]

        # Try to extract from the abbr element
        date_element = comment.find('abbr', first=True)
        if date_element:
            date = utils.parse_datetime(date_element.text, search=True)
            if not date:
                logger.debug(f"Unable to parse {date_element.text}")
        else:
            date = None

        image_url = comment.find('a[href^="https://lm.facebook.com/l.php"]', first=True)
        if image_url:
            image_url = parse_qs(urlparse(image_url.attrs["href"]).query).get("u")[0]
        else:
            image_url = comment.find('i.img:not(.profpic)[style]', first=True)
            if image_url:
                match = self.image_regex_lq.search(image_url.attrs["style"])
                if match:
                    image_url = utils.decode_css_url(match.groups()[0])

        reactions = {}
        comment_reactors_opt = self.options.get(
            "comment_reactors", self.options.get("reactions") or self.options.get("reactors")
        )
        if comment_reactors_opt:
            self.options["reactors"] = True  # Required for comment reaction extraction
            reactors = comment.find(
                'a[href^="/ufi/reaction/profile/browser/?ft_ent_identifier="] i,'
                'a[href^="/ufi/reaction/profile/browser/?ft_ent_identifier="] img',
                first=True,
            )
            if reactors:
                reactions = self.extract_reactions(comment_id, force_parse_HTML=True)
                if comment_reactors_opt != "generator":
                    reactions["reactors"] = utils.safe_consume(reactions.get("reactors", []))

        return {
            "comment_id": comment_id,
            "comment_url": utils.urljoin(FB_BASE_URL, comment_id),
            "commenter_id": commenter_id,
            "commenter_url": url,
            "commenter_name": name,
            "commenter_meta": commenter_meta,
            "comment_text": text,
            "comment_time": date,
            "comment_image": image_url,
            "comment_reactors": reactions.get("reactors", []),
            "comment_reactions": reactions.get("reactions"),
            "comment_reaction_count": reactions.get("reaction_count"),
        }

    def extract_comment_replies(self, replies_url):
        if not self.options.get("progress"):
            logger.debug(f"Fetching {replies_url}")
        try:
            # Some users have to use an AJAX POST method to get replies.
            # Check if this is the case by checking for the element that holds the encrypted response token
            use_ajax_post = self.full_post_html.find("input[name='fb_dtsg']", first=True) is not None

            if use_ajax_post:
                fb_dtsg = self.full_post_html.find("input[name='fb_dtsg']", first=True).attrs["value"]
                encryptedAjaxResponseToken = re.search(
                    r'encrypted":"([^"]+)', self.full_post_html.html
                ).group(1)
                response = self.request(
                    replies_url,
                    post=True,
                    params={"fb_dtsg": fb_dtsg, "__a": encryptedAjaxResponseToken},
                )
            else:
                use_ajax_post = False
                response = self.request(replies_url)
            
        except exceptions.TemporarilyBanned:
            raise
        except Exception as e:
            logger.error(e)
            return

        if use_ajax_post:
            prefix_length = len('for (;;);')
            data = json.loads(response.text[prefix_length:])  # Strip 'for (;;);'
            for action in data['payload']['actions']:
                if action["cmd"] == "replace":
                    html = utils.make_html_element(
                        action['html'],
                        url=FB_MOBILE_BASE_URL,
                    )
                    break

            reply_selector = 'div[data-sigil="comment inline-reply"]'
        
            if self.options.get("noscript"):
                reply_selector = '#root div[id]'
            replies = html.find(reply_selector)
        
        else:
            # Skip first element, as it will be this comment itself
            reply_selector = 'div[data-sigil="comment"]'
            replies = response.html.find(reply_selector)[1:]
            
        try:
            for reply in replies:
                yield self.parse_comment(reply)
        except exceptions.TemporarilyBanned:
            raise
        except Exception as e:
            logger.error(f"Unable to parse comment {replies_url} replies {replies}: {e}")
            
    def extract_comment_with_replies(self, comment):
        try:
            result = self.parse_comment(comment)
            result["replies"] = [
                self.parse_comment(reply)
                for reply in comment.find("div[data-sigil='comment inline-reply']")
            ]
            replies_url = comment.find(
                "div.async_elem[data-sigil='replies-see-more'] a[href],div[id*='comment_replies_more'] a[href]",
                first=True,
            )
            if replies_url:
                reply_generator = self.extract_comment_replies(replies_url.attrs["href"])
                if result["replies"]:
                    result["replies"] = itertools.chain(result["replies"], reply_generator)
                else:
                    result["replies"] = reply_generator
            return result
        except exceptions.TemporarilyBanned:
            raise
        except Exception as e:
            logger.error(f"Unable to parse comment {comment}: {e}")

    def extract_comments_full(self):
        """Fetch comments for an existing post obtained by `get_posts`.
        Note that this method may raise multiple http requests per post to get all comments"""
        if not self.full_post_html:
            logger.error("Unable to get comments without full post HTML")
            return
        comments_area_selector = 'div[id^="ufi_"]'
        elem = self.full_post_html.find(comments_area_selector, first=True)
        if not elem:
            logger.error("No comments area found")
            return
        comments_selector = 'div[data-sigil="comment"]'
        if self.options.get("noscript"):
            comments_selector = f"{comments_area_selector}>div>div:not(id)>div"
        comments = list(elem.find(comments_selector))
        if not comments:
            logger.warning("No comments found on page")
            return

        for comment in comments:
            result = self.extract_comment_with_replies(comment)
            if result:
                yield result

        more_selector = f"div#see_next_{self.post.get('post_id')} a"
        more = elem.find(more_selector, first=True)
        if not more:
            more_selector = f"div#see_prev_{self.post.get('post_id')} a"
            more = elem.find(more_selector, first=True)

        # Comment limiting and progress
        limit = 1e9  # Default
        if more and more.attrs.get("data-ajaxify-href"):
            parsed = parse_qs(urlparse(more.attrs.get("data-ajaxify-href")).query)
            count = int(parsed.get("count")[0])
            if count < limit:
                limit = count
        comments_opt = self.options.get('comments')
        if type(comments_opt) in [int, float]:
            limit = comments_opt
        logger.debug(f"Fetching up to {limit} comments")

        if self.options.get("progress"):
            pbar = tqdm(total=limit)

        visited_urls = []
        request_url_callback = self.options.get('comment_request_url_callback')
        more_url = None
        if more:
            if self.options.get("response_url"):
                more_url = utils.combine_url_params(
                    self.options.get("response_url"), more.attrs.get("href")
                )
            else:
                more_url = (
                    more.attrs.get("href")
                    + "&m_entstream_source=video_home&player_suborigin=entry_point&player_format=permalink"
                )
        if self.options.get("comment_start_url"):
            more_url = self.options.get("comment_start_url")

        while more_url and len(comments) <= limit:
            if request_url_callback:
                request_url_callback(utils.urljoin(FB_MOBILE_BASE_URL, more_url))
            if more_url in visited_urls:
                logger.debug("cycle detected, break")
                break
            if self.options.get("progress"):
                pbar.update(30)
            else:
                logger.debug(f"Fetching {more_url}")
            try:
                response = self.request(more_url)
            except exceptions.TemporarilyBanned:
                raise
            except Exception as e:
                logger.error(e)
                break
            visited_urls.append(more_url)
            elem = response.html.find(comments_area_selector, first=True)
            if not elem:
                logger.warning("No comments found on page")
                break
            more_comments = elem.find(comments_selector)
            comments.extend(more_comments)
            if not more_comments:
                logger.warning("No comments found on page")
                break
            for comment in more_comments:
                result = self.extract_comment_with_replies(comment)
                if result:
                    yield result
            more = elem.find(more_selector, first=True)
            if more:
                if self.options.get("response_url"):
                    more_url = utils.combine_url_params(
                        self.options.get("response_url"), more.attrs.get("href")
                    )
                else:
                    more_url = (
                        more.attrs.get("href")
                        + "&m_entstream_source=video_home&player_suborigin=entry_point&player_format=permalink"
                    )
            else:
                more_url = None

    def parse_share_and_reactions(self, html: str):
        bad_jsons = self.shares_and_reactions_regex.findall(html)
        for bad_json in bad_jsons:
            good_json = self.bad_json_key_regex.sub(r'\g<prefix>"\g<key>":', bad_json)
            yield json.loads(good_json)

    def extract_listing(self) -> PartialPost:
        # Marketplace listings
        divs = self.element.find("div[data-ft='{\"tn\":\"H\"}']>div>div")
        if len(divs) >= 3:
            return {
                "listing_title": divs[0].find("span")[-1].text,
                "listing_price": divs[1].text,
                "listing_location": divs[2].text,
            }

    def extract_with(self) -> PartialPost:
        # Header is like "user is with other_user and n others"
        links = self.element.find("header h3 a")[1:]
        if links:
            people = [{"name": links[0].text, "link": links[0].attrs["href"]}]
            url = links[-1].attrs["href"]
            if url.startswith("/browse/users/"):
                logger.debug(f"Fetching {url}")
                response = self.request(url)
                links = response.html.find("#root .item>div>div>a:not(.touchable)")
                for link in links:
                    people.append({"name": link.text, "link": link.attrs["href"]})
            return {"with": people, "header": self.element.find("header h3", first=True).text}

    @property
    def data_ft(self) -> dict:
        if self._data_ft is not None:
            return self._data_ft

        self._data_ft = {}
        try:
            data_ft_json = self.element.attrs['data-ft'].replace("\\\\", "\\")
            self._data_ft = demjson.decode(data_ft_json)
        except JSONDecodeError as ex:
            logger.error("Error parsing data-ft JSON: %r", ex)
        except KeyError:
            logger.error("data-ft attribute not found")

        return self._data_ft

    @property
    def full_post_html(self):
        if self._full_post_html is not None:
            return self._full_post_html

        if self.options.get("allow_extra_requests", True) and self.post.get('post_id'):
            url = self.post.get('post_id')
            logger.debug(f"Fetching {url}")
            try:
                response = self.request(url)
            except exceptions.NotFound as e:
                url = self.post.get('post_url').replace(FB_BASE_URL, FB_MOBILE_BASE_URL)
                logger.debug(f"Fetching {url}")
                response = self.request(url)
            if response.text.startswith("for (;;)"):
                logger.warning("full_post_html startswith for (;;)")
            self._full_post_html = response.html
            return self._full_post_html
        else:
            return None

    @property
    def live_data(self):
        if self._live_data:
            return self._live_data
        try:
            self._live_data = self.get_jsmod("MLiveData")
        except Exception as e:
            logger.error(e)
        return self._live_data

    def get_jsmod(self, name, element=None):
        if not element:
            if self.full_post_html:
                element = self.full_post_html
            else:
                element = self.element
        match = re.search(name + r'[^{]+({.+?})(?:\]\]|,\d)', element.html)
        if match:
            # Use demjson to load JS, as unquoted keys is not valid JSON
            return demjson.decode(match.group(1))
        else:
            return {}


class GroupPostExtractor(PostExtractor):
    """Class for extracting posts from Facebook Groups rather than Pages"""

    post_url_regex = re.compile(r'https://m.facebook.com/groups/[^/]+/permalink/')
    post_story_regex = re.compile(r'href="(https://m.facebook.com/groups/[^/]+/permalink/\d+/)')


class PhotoPostExtractor(PostExtractor):
    def extract_text(self) -> PartialPost:
        text = self.element.find("div.msg", first=True).text
        return {"text": text, "post_text": text}

    def extract_photo_link(self) -> PartialPost:
        image = self.extract_photo_link_HQ(self.full_post_html.html)
        return {
            "image": image,
            "images": [image],
            "images_description": self.extract_image_lq()["images_lowquality_description"],
        }

    def extract_user_id(self) -> PartialPost:
        match = re.search(r'entity_id:(\d+),', self.element.html)
        if match:
            return {"user_id": match.group(1)}

    def extract_post_url(self) -> PartialPost:
        return {"post_url": utils.urljoin(FB_MOBILE_BASE_URL, self.extract_post_id()["post_id"])}

    def extract_post_id(self) -> PartialPost:
        try:
            return {"post_id": str(self.live_data["ft_ent_identifier"])}
        except KeyError:
            match = re.search(r'ft_ent_identifier=(\d+)', self.full_post_html.html)
            if match:
                return {"post_id": match.groups()[0]}


class HashtagPostExtractor(PostExtractor):
    def __init__(self, element, options, request_fn, full_post_html=None):
        post_id = self.extract_hashtag_post_id(element)
        if post_id:
            response = request_fn(post_id)
            if response:
                element = response.html.find('[data-ft*="top_level_post_id"]')[0]
                full_post_html = response.html

        super().__init__(element, options, request_fn, full_post_html)

    def extract_hashtag_post_id(self, element):
        match = re.search(r'ft_ent_identifier=(\d+)', element.html)
        if match:
            return match.groups()[0]
        return None


class StoryExtractor(PostExtractor):
    def extract_username(self) -> PartialPost:
        elem = self.element.find('#m-stories-card-header', first=True)
        if elem:
            url = elem.find("a", first=True).attrs["href"]
            if url:
                url = utils.urljoin(FB_BASE_URL, url)
            return {'username': elem.find("div.overflowText", first=True).text, 'user_url': url}

    def extract_time(self) -> PartialPost:
        date_element = self.element.find("abbr[data-store*='time']", first=True)
        time = json.loads(date_element.attrs["data-store"])["time"]
        logger.debug(f"Got exact timestamp from abbr[data-store]: {datetime.fromtimestamp(time)}")
        return {'time': datetime.fromtimestamp(time), 'timestamp': time}
