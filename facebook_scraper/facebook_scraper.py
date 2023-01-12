import itertools
import logging
from urllib.parse import urljoin
import warnings
import re
from functools import partial
from typing import Iterator, Union
import json
import demjson3 as demjson
from urllib.parse import parse_qs, urlparse, unquote
from datetime import datetime
import os

from requests import RequestException
from requests_html import HTMLSession

from . import utils
from .constants import (
    DEFAULT_PAGE_LIMIT,
    FB_BASE_URL,
    FB_MOBILE_BASE_URL,
    FB_W3_BASE_URL,
    FB_MBASIC_BASE_URL,
)
from .extractors import (
    extract_group_post,
    extract_post,
    extract_photo_post,
    extract_story_post,
    PostExtractor,
    extract_hashtag_post,
)
from .fb_types import Post, Profile
from .page_iterators import (
    iter_group_pages,
    iter_pages,
    iter_photos,
    iter_search_pages,
    iter_hashtag_pages,
)
from . import exceptions


logger = logging.getLogger(__name__)


class FacebookScraper:
    """Class for creating FacebookScraper Iterators"""

    base_url = FB_MOBILE_BASE_URL
    default_headers = {
        "Accept": "*/*",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip,deflate",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    }
    have_checked_locale = False

    def __init__(self, session=None, requests_kwargs=None):
        if session is None:
            session = HTMLSession()
            session.headers.update(self.default_headers)

        if requests_kwargs is None:
            requests_kwargs = {}

        self.session = session
        self.requests_kwargs = requests_kwargs
        self.request_count = 0

    def set_user_agent(self, user_agent):
        self.session.headers["User-Agent"] = user_agent

    def set_noscript(self, noscript):
        if noscript:
            self.session.cookies.set("noscript", "1")
        else:
            self.session.cookies.set("noscript", "0")

    def set_proxy(self, proxy, verify=True):
        self.requests_kwargs.update(
            {'proxies': {'http': proxy, 'https': proxy}, 'verify': verify}
        )
        ip = self.get(
            "http://lumtest.com/myip.json", headers={"Accept": "application/json"}
        ).json()
        logger.debug(f"Proxy details: {ip}")

    def get_posts(self, account: str, **kwargs) -> Iterator[Post]:
        kwargs["scraper"] = self
        iter_pages_fn = partial(iter_pages, account=account, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_reactors(self, post_id: int, **kwargs) -> Iterator[dict]:
        reaction_url = (
            f'https://m.facebook.com/ufi/reaction/profile/browser/?ft_ent_identifier={post_id}'
        )
        logger.debug(f"Fetching {reaction_url}")
        response = self.get(reaction_url)
        extractor = PostExtractor(response.html, kwargs, self.get, full_post_html=response.html)
        return extractor.extract_reactors(response)

    def get_photos(self, account: str, **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_photos, account=account, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_posts_by_hashtag(self, hashtag: str, **kwargs) -> Iterator[Post]:
        kwargs["scraper"] = self
        kwargs["base_url"] = FB_MBASIC_BASE_URL
        iter_pages_fn = partial(
            iter_hashtag_pages, hashtag=hashtag, request_fn=self.get, **kwargs
        )
        return self._generic_get_posts(extract_hashtag_post, iter_pages_fn, **kwargs)

    def get_posts_by_url(self, post_urls, options={}, remove_source=True) -> Iterator[Post]:
        if self.session.cookies.get("noscript") == "1":
            options["noscript"] = True
        for post_url in post_urls:
            url = str(post_url)
            if url.startswith(FB_BASE_URL):
                url = url.replace(FB_BASE_URL, FB_MOBILE_BASE_URL)
            if url.startswith(FB_W3_BASE_URL):
                url = url.replace(FB_W3_BASE_URL, FB_MOBILE_BASE_URL)
            if not url.startswith(FB_MOBILE_BASE_URL):
                url = utils.urljoin(FB_MOBILE_BASE_URL, url)

            post = {"original_request_url": post_url, "post_url": url}
            logger.debug(f"Requesting page from: {url}")
            response = self.get(url)
            options["response_url"] = response.url
            photo_post = False
            if "/stories/" in url or "/story/" in url:
                elem = response.html.find("#story_viewer_content", first=True)
            else:
                elem = response.html.find('[data-ft*="top_level_post_id"]', first=True)
                if not elem:
                    elem = response.html.find('div.async_like', first=True)
                if response.html.find("div.msg", first=True):
                    photo_post = True
                    elem = response.html
            if not elem:
                logger.warning("No raw posts (<article> elements) were found in this page.")
            else:
                comments_area = response.html.find('div.ufi', first=True)
                if comments_area:
                    # Makes likes/shares regexes work
                    try:
                        elem = utils.make_html_element(
                            elem.html.replace("</footer>", comments_area.html + "</footer>")
                        )
                    except ValueError as e:
                        logger.debug(e)

                if photo_post:
                    post.update(
                        extract_photo_post(
                            elem,
                            request_fn=self.get,
                            options=options,
                            full_post_html=response.html,
                        )
                    )
                elif url.startswith(utils.urljoin(FB_MOBILE_BASE_URL, "/groups/")):
                    post.update(
                        extract_group_post(
                            elem,
                            request_fn=self.get,
                            options=options,
                            full_post_html=response.html,
                        )
                    )
                elif "/stories/" in url or "/story/" in url:
                    post.update(
                        extract_story_post(
                            elem,
                            request_fn=self.get,
                            options=options,
                            full_post_html=response.html,
                        )
                    )
                else:
                    post.update(
                        extract_post(
                            elem,
                            request_fn=self.get,
                            options=options,
                            full_post_html=response.html,
                        )
                    )
                if not post.get("post_url"):
                    post["post_url"] = url
                if remove_source:
                    post.pop('source', None)
            yield post

    def get_posts_by_search(self, word: str, **kwargs) -> Iterator[Post]:
        kwargs["scraper"] = self
        iter_pages_fn = partial(iter_search_pages, word=word, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_friends(self, account, **kwargs) -> Iterator[Profile]:
        friend_opt = kwargs.get("friends")
        limit = None
        if type(friend_opt) in [int, float]:
            limit = friend_opt
        friend_url = kwargs.pop("start_url", None)
        if not friend_url:
            friend_url = utils.urljoin(FB_MOBILE_BASE_URL, f'/{account}/friends/')
        request_url_callback = kwargs.get('request_url_callback')
        friends_found = 0
        while friend_url:
            logger.debug(f"Requesting page from: {friend_url}")
            response = self.get(friend_url)
            elems = response.html.find('div[class="timeline"] > div > div')
            logger.debug(f"Found {len(elems)} friends")
            for elem in elems:
                name = elem.find("h3>a,h1>a", first=True)
                if not name:
                    continue
                # Tagline
                tagline = elem.find("span.fcg", first=True)
                if tagline:
                    tagline = tagline.text
                else:
                    tagline = ""
                # Profile Picture
                profile_picture = elem.find("i.profpic", first=True).attrs.get("style")
                match = re.search(r"url\('(.+)'\)", profile_picture)
                if match:
                    profile_picture = utils.decode_css_url(match.groups()[0])
                # User ID if present, not present if no "add friend"
                user_id = elem.find("a.touchable[data-store]", first=True)
                if user_id:
                    user_id = json.loads(user_id.attrs["data-store"]).get("id")
                else:
                    user_id = ""

                friend = {
                    "id": user_id,
                    "link": name.attrs.get("href"),
                    "name": name.text,
                    "profile_picture": profile_picture,
                    "tagline": tagline,
                }
                yield friend
                friends_found += 1
            if limit and friends_found > limit:
                return
            more = re.search(r'm_more_friends",href:"([^"]+)"', response.text)
            if more:
                friend_url = utils.urljoin(FB_MOBILE_BASE_URL, more.group(1))
                if request_url_callback:
                    request_url_callback(friend_url)
            else:
                return

    def get_collection(self, more_url, limit=None, **kwargs) -> Iterator[Profile]:
        request_url_callback = kwargs.get('request_url_callback')
        count = 0
        while more_url:
            logger.debug(f"Requesting page from: {more_url}")
            response = self.get(more_url)
            if response.text.startswith("for (;;);"):
                prefix_length = len('for (;;);')
                data = json.loads(response.text[prefix_length:])  # Strip 'for (;;);'
                for action in data['payload']['actions']:
                    if action['cmd'] == 'append' and action['html']:
                        element = utils.make_html_element(
                            action['html'],
                            url=FB_MOBILE_BASE_URL,
                        )
                        elems = element.find('a.touchable')
                        html = element.text
                    elif action['cmd'] == 'script':
                        more_url = re.search(
                            r'("\\/timeline\\/app_collection\\/more\\/[^"]+")', action["code"]
                        )
                        if more_url:
                            more_url = more_url.group(1)
                            more_url = json.loads(more_url)
            else:
                elems = response.html.find('#timelineBody a.touchable')
                more_url = re.search(
                    r'href:"(/timeline/app_collection/more/[^"]+)"', response.text
                )
                if more_url:
                    more_url = more_url.group(1)
            logger.debug(f"Found {len(elems)} elems")
            for elem in elems:
                name = elem.find("strong", first=True).text
                link = elem.attrs.get("href")
                try:
                    tagline = elem.find("div.twoLines", first=True).text
                except:
                    tagline = None
                profile_picture = elem.find("i.profpic", first=True).attrs.get("style")
                match = re.search(r"url\('(.+)'\)", profile_picture)
                if match:
                    profile_picture = utils.decode_css_url(match.groups()[0])
                result = {
                    "link": link,
                    "name": name,
                    "profile_picture": profile_picture,
                    "tagline": tagline,
                }
                yield result
                count += 1
            if type(limit) in [int, float] and count > limit:
                return
            if more_url and request_url_callback:
                request_url_callback(more_url)

    def get_profile(self, account, **kwargs) -> Profile:
        account = account.replace("profile.php?id=", "")
        result = {}

        if kwargs.get("allow_extra_requests", True):
            logger.debug(f"Requesting page from: {account}")
            response = self.get(account)
            try:
                top_post = response.html.find(
                    '[data-ft*="top_level_post_id"]:not([data-sigil="m-see-translate-link"])',
                    first=True,
                )
                assert top_post is not None
                top_post = PostExtractor(top_post, kwargs, self.get).extract_post()
                top_post.pop("source")
                result["top_post"] = top_post
            except Exception as e:
                logger.error(f"Unable to extract top_post {type(e)}:{e}")

            try:
                result["Friend_count"] = utils.parse_int(
                    response.html.find("a[data-store*='friends']>div>div")[-1].text.split()[0]
                )
            except Exception as e:
                result["Friend_count"] = None
                logger.error(f"Friend_count extraction failed: {e}")
            try:
                result["Follower_count"] = utils.parse_int(
                    response.html.find(
                        "div[data-sigil*='profile-intro-card-log']",
                        containing="Followed by",
                        first=True,
                    ).text
                )
            except Exception as e:
                result["Follower_count"] = None
                logger.error(f"Follower_count extraction failed: {e}")
            try:
                following_url = f'/{account}?v=following'
                logger.debug(f"Fetching {following_url}")
                following_response = self.get(following_url)
                result["Following_count"] = utils.parse_int(
                    following_response.html.find("div[role='heading']", first=True).text
                )
            except Exception as e:
                result["Following_count"] = None
                logger.error(f"Following_count extraction failed: {e}")

            photo_links = response.html.find("a[href^='/photo.php']")
            if len(photo_links) == 1:
                profile_photo = photo_links[0]
                response = self.get(profile_photo.attrs.get("href"))
                extractor = PostExtractor(response.html, kwargs, self.get)
                result["profile_picture"] = extractor.extract_photo_link_HQ(response.html.html)
            elif len(photo_links) >= 2:
                cover_photo = photo_links[0]
                result["cover_photo_text"] = cover_photo.attrs.get("title")
                # Check if there is a cover photo or not
                if result["cover_photo_text"] is not None:
                    response = self.get(cover_photo.attrs.get("href"))
                    extractor = PostExtractor(response.html, kwargs, self.get)
                    result["cover_photo"] = extractor.extract_photo_link_HQ(response.html.html)

                    profile_photo = photo_links[1]
                    response = self.get(profile_photo.attrs.get("href"))
                    result["profile_picture"] = extractor.extract_photo_link_HQ(
                        response.html.html
                    )
                else:
                    result["cover_photo"] = None
                    profile_photo = photo_links[0]
                    response = self.get(profile_photo.attrs.get("href"))
                    extractor = PostExtractor(response.html, kwargs, self.get)
                    result["profile_picture"] = extractor.extract_photo_link_HQ(
                        response.html.html
                    )
            else:
                cover_photo = response.html.find(
                    "div[data-sigil='cover-photo']>i.img", first=True
                )
                if cover_photo:
                    match = re.search(r"url\('(.+)'\)", cover_photo.attrs["style"])
                    if match:
                        result["cover_photo"] = utils.decode_css_url(match.groups()[0])
                profpic = response.html.find("img.profpic", first=True)
                if profpic:
                    result["profile_picture"] = profpic.attrs["src"]

        about_url = utils.urljoin(FB_MOBILE_BASE_URL, f'/{account}/about/')
        logger.debug(f"Requesting page from: {about_url}")
        response = self.get(about_url)
        match = re.search(r'entity_id:(\d+)', response.html.html)
        if match:
            result["id"] = match.group(1)
        # Profile name is in the title
        title = response.html.find("title", first=True).text
        if " | " in title:
            title = title.split(" | ")[0]
        result["Name"] = title

        about = response.html.find("div#main_column,div.aboutme", first=True)
        if not about:
            logger.warning("No about section found")
            return result
        for card in about.find("div[data-sigil='profile-card']"):
            header = card.find("header", first=True).text
            if header.startswith("About"):
                header = "About"  # Truncate strings like "About Mark"
            if header in ["Work, Education"]:
                experience = []
                for elem in card.find("div.experience"):
                    xp = {}
                    try:
                        xp["link"] = elem.find("a", first=True).attrs["href"]
                    except:
                        pass
                    bits = elem.text.split("\n")
                    if len(bits) == 2:
                        xp["text"], xp["type"] = bits
                    elif len(bits) == 3:
                        xp["text"], xp["type"], xp["year"] = bits
                    else:
                        xp["text"] = elem.text
                    experience.append(xp)
                result[header] = experience
            elif header == "Places lived":
                places = []
                for elem in card.find("div.touchable"):
                    place = {}
                    try:
                        place["link"] = elem.find("a", first=True).attrs["href"]
                    except:
                        pass
                    if "\n" in elem.text:
                        place["text"], place["type"] = elem.text.split("\n")
                    else:
                        place["text"] = elem.text
                    places.append(place)
                result[header] = places
            else:
                bits = card.text.split("\n")[1:]  # Remove header
                if len(bits) >= 3 and header == "Relationship":
                    result[header] = {"to": bits[0], "type": bits[1], "since": bits[2]}
                elif len(bits) == 1:
                    result[header] = bits[0]
                elif (
                    header
                    in [
                        "Contact Info",
                        "Basic Info",
                        "Education",
                        "Family Members",
                        "Other names",
                    ]
                    and len(bits) % 2 == 0
                ):  # Divisible by two, assume pairs
                    pairs = {}
                    for i in range(0, len(bits), 2):
                        if bits[i + 1] == "Websites":
                            if "Websites" not in pairs:
                                pairs["Websites"] = []
                            pairs["Websites"].append(bits[i])
                        else:
                            pairs[bits[i + 1]] = bits[i]
                    result[header] = pairs
                else:
                    result[header] = "\n".join(bits)
        if kwargs.get("friends"):
            result["Friends"] = list(self.get_friends(account, **kwargs))
        if kwargs.get("followers"):
            result["Followers"] = list(
                self.get_collection(
                    f'/{account}?v=followers', limit=kwargs.get("followers"), **kwargs
                )
            )
        if kwargs.get("following"):
            result["Following"] = list(
                self.get_collection(
                    f'/{account}?v=following', limit=kwargs.get("following"), **kwargs
                )
            )

        # Likes
        if result.get("id") and kwargs.get("likes"):
            likes_url = utils.urljoin(
                FB_MOBILE_BASE_URL,
                f'timeline/app_section/?section_token={result["id"]}:2409997254',
            )
            logger.debug(f"Requesting page from: {likes_url}")
            response = self.get(likes_url)
            result["likes_by_category"] = {}
            for elem in response.html.find('header[data-sigil="profile-card-header"]'):
                count, category = elem.text.split("\n")
                count = utils.parse_int(count)
                if category == "All Likes":
                    result["likes_count"] = count
                result["likes_by_category"][category] = count

            all_likes_url = utils.urljoin(
                FB_MOBILE_BASE_URL,
                f'timeline/app_collection/?collection_token={result["id"]}:2409997254:96',
            )
            logger.debug(f"Requesting page from: {all_likes_url}")
            response = self.get(all_likes_url)
            result["likes"] = []
            for elem in response.html.find("div._1a5p"):
                result["likes"].append(
                    {
                        "name": elem.text,
                        "link": elem.find("a", first=True).attrs.get("href"),
                    }
                )
            more_url = re.search(r'href:"(/timeline/app_collection/more/[^"]+)"', response.text)
            if more_url:
                more_url = more_url.group(1)
            while more_url:
                logger.debug(f"Fetching {more_url}")
                response = self.get(more_url)
                prefix_length = len('for (;;);')
                data = json.loads(response.text[prefix_length:])  # Strip 'for (;;);'
                for action in data['payload']['actions']:
                    if action['cmd'] == 'append' and action['html']:
                        element = utils.make_html_element(
                            action['html'],
                            url=FB_MOBILE_BASE_URL,
                        )
                        for elem in element.find("div._1a5p"):
                            result["likes"].append(
                                {
                                    "name": elem.text,
                                    "link": elem.find("a", first=True).attrs.get("href"),
                                }
                            )
                    elif action['cmd'] == 'script':
                        more_url = re.search(
                            r'("\\/timeline\\/app_collection\\/more\\/[^"]+")', action["code"]
                        )
                        if more_url:
                            more_url = more_url.group(1)
                            more_url = json.loads(more_url)

        return result

    def get_page_reviews(self, page, **kwargs) -> Iterator[Post]:
        more_url = f"/{page}/reviews"
        while more_url:
            logger.debug(f"Fetching {more_url}")
            response = self.get(more_url)
            if response.text.startswith("for (;;);"):
                prefix_length = len('for (;;);')
                data = json.loads(response.text[prefix_length:])  # Strip 'for (;;);'
                for action in data['payload']['actions']:
                    if action['cmd'] == 'replace' and action['html']:
                        element = utils.make_html_element(
                            action['html'],
                            url=FB_MOBILE_BASE_URL,
                        )
                        elems = element.find('#page_suggestions_on_liking ~ div')
                    elif action['cmd'] == 'script':
                        more_url = re.search(
                            r'see_more_cards_id","href":"([^"]+)"', action["code"]
                        )
                        if more_url:
                            more_url = more_url.group(1)
                            more_url = utils.decode_css_url(more_url)
                            more_url = more_url.replace("\\", "")
            else:
                elems = response.html.find('#page_suggestions_on_liking ~ div')
                more_url = re.search(r'see_more_cards_id",href:"([^"]+)"', response.text)
                if more_url:
                    more_url = more_url.group(1)

            for elem in elems:
                header_elem = elem.find("div[data-nt='FB:TEXT4']:has(span)", first=True)
                if not header_elem:
                    continue
                bits = list(header_elem.element.itertext())
                username = bits[0].strip()
                recommends = "recommends" in header_elem.text
                links = header_elem.find("a")
                if len(links) == 2:
                    user_url = utils.urljoin(FB_BASE_URL, links[0].attrs["href"])
                else:
                    user_url = None
                text_elem = elem.find("div[data-nt='FB:FEED_TEXT'] span p", first=True)
                if text_elem:
                    text = text_elem.text
                else:
                    text = None
                date_element = elem.find("abbr[data-store*='time']", first=True)
                time = json.loads(date_element.attrs["data-store"])["time"]
                yield {
                    "user_url": user_url,
                    "username": username,
                    "profile_picture": elem.find("img", first=True).attrs["src"],
                    "text": text,
                    "header": header_elem.text,
                    "time": datetime.fromtimestamp(time),
                    "timestamp": time,
                    "recommends": recommends,
                    "post_url": utils.urljoin(
                        FB_BASE_URL, elem.find("a[href*='story']", first=True).attrs["href"]
                    ),
                }

    def get_page_info(self, page, **kwargs) -> Profile:
        result = {}
        desc = None

        try:
            about_url = f'/{page}/about/'
            logger.debug(f"Requesting page from: {about_url}")
            resp = self.get(about_url)
            result["name"] = resp.html.find("title", first=True).text.replace(" - About", "")
            desc = resp.html.find("meta[name='description']", first=True)
            result["about"] = resp.html.find(
                '#pages_msite_body_contents,div.aboutme', first=True
            ).text
            cover_photo = resp.html.find("#msite-pages-header-contents i.coverPhoto", first=True)
            if cover_photo:
                match = re.search(r"url\('(.+)'\)", cover_photo.attrs["style"])
                if match:
                    result["cover_photo"] = utils.decode_css_url(match.groups()[0])
            profile_photo = resp.html.find("#msite-pages-header-contents img", first=True)
            if profile_photo:
                result["profile_photo"] = profile_photo.attrs["src"]
        except Exception as e:
            logger.error(e)
        try:
            url = f'/{page}/'
            logger.debug(f"Requesting page from: {url}")
            resp = self.get(url)
            result["id"] = re.search(r'pages/transparency/(\d+)', resp.html.html).group(1)
            result["name"] = resp.html.find("title", first=True).text.replace(" - Home", "")
            desc = resp.html.find("meta[name='description']", first=True)
            ld_json = None
            try:
                ld_json = resp.html.find("script[type='application/ld+json']", first=True).text
            except:
                logger.error("No ld+json element")
                url = f'/{page}/community'
                logger.debug(f"Requesting page from: {url}")
                try:
                    community_resp = self.get(url)
                    try:
                        ld_json = community_resp.html.find(
                            "script[type='application/ld+json']", first=True
                        ).text
                    except:
                        logger.error("No ld+json element")
                        likes_and_follows = community_resp.html.find(
                            "#page_suggestions_on_liking+div", first=True
                        ).text.split("\n")
                        result["followers"] = utils.convert_numeric_abbr(likes_and_follows[2])
                except:
                    pass
            if ld_json:
                meta = demjson.decode(ld_json)
                result.update(meta["author"])
                result["type"] = result.pop("@type")
                for interaction in meta.get("interactionStatistic", []):
                    if interaction["interactionType"] == "http://schema.org/FollowAction":
                        result["followers"] = interaction["userInteractionCount"]
            try:
                result["about"] = resp.html.find(
                    '#pages_msite_body_contents>div>div:nth-child(2)', first=True
                ).text
            except Exception as e:
                logger.error(e)
                result = self.get_profile(page)
            for elem in resp.html.find("div[data-sigil*='profile-intro-card-log']"):
                text = elem.text.split("\n")[0]
                if " Followers" in text:
                    result["followers"] = utils.convert_numeric_abbr(
                        text.replace(" Followers", "")
                    )
                if text.startswith("Price Range"):
                    result["Price Range"] = text.split(" · ")[-1]
                link = elem.find("a[href]", first=True)
                if link:
                    link = link.attrs["href"]
                    if "active_ads" in link:
                        result["active_ads_link"] = link
                    if "maps.google.com" in link:
                        result["map_link"] = parse_qs(urlparse(link).query).get("u")[0]
                        result["address"] = text
                    if link.startswith("tel:"):
                        result["phone"] = link.replace("tel:", "")
                    if link.startswith("mailto:"):
                        result["email"] = link.replace("mailto:", "")
            result["rating"] = resp.html.find("div[data-nt='FB:TEXT4']")[1].text
        except Exception as e:
            logger.error(e)
        if desc:
            logger.debug(desc.attrs["content"])
            match = re.search(r'\..+?(\d[\d,.]+).+·', desc.attrs["content"])
            if match:
                result["likes"] = utils.parse_int(match.groups()[0])
            bits = desc.attrs["content"].split("·")
            if len(bits) == 3:
                result["people_talking_about_this"] = utils.parse_int(bits[1])
                result["checkins"] = utils.parse_int(bits[2])
        if kwargs.get("reviews"):
            result["reviews"] = self.get_page_reviews(page, **kwargs)
            if kwargs.get("reviews") != "generator":
                result["reviews"] = utils.safe_consume(result["reviews"])

        return result

    def get_group_info(self, group, **kwargs) -> Profile:
        self.set_user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8"
        )
        url = f'/groups/{group}'
        logger.debug(f"Requesting page from: {url}")
        resp = self.get(url).html
        try:
            url = resp.find("a[href*='?view=info']", first=True).attrs["href"]
            url += "&sfd=1"  # Add parameter to get full "about"-text
        except AttributeError:
            raise exceptions.UnexpectedResponse("Unable to resolve view=info URL")
        logger.debug(f"Requesting page from: {url}")
        resp = self.get(url).html
        result = {}
        result["id"] = re.search(r'/groups/(\d+)', url).group(1)
        try:
            result["name"] = resp.find("header h3", first=True).text
            result["type"] = resp.find("header div", first=True).text
            members = resp.find("div[data-testid='m_group_sections_members']", first=True)
            result["members"] = utils.parse_int(members.text)
        except AttributeError:
            raise exceptions.UnexpectedResponse("Unable to get one of name, type, or members")

        # Try to extract the group description
        try:
            # Directly tageting the weird generated class names is not optimal, but it's the best i could do.
            about_div = resp.find("._52jc._55wr", first=True)

            # Removing the <wbr>-tags that are converted to linebreaks by .text
            from requests_html import HTML

            no_word_breaks = HTML(html=about_div.html.replace("<wbr/>", ""))

            result["about"] = no_word_breaks.text
        except:
            result["about"] = None

        try:
            url = members.find("a", first=True).attrs.get("href")
            logger.debug(f"Requesting page from: {url}")

            resp = self.get(url).html
            url = resp.find("a[href*='listType=list_admin_moderator']", first=True)
            if kwargs.get("admins", True):
                if url:
                    url = url.attrs.get("href")
                    logger.debug(f"Requesting page from: {url}")
                    try:
                        respAdmins = self.get(url).html
                    except:
                        raise exceptions.UnexpectedResponse("Unable to get admin list")
                else:
                    respAdmins = resp
                # Test if we are a member that can add new members
                if re.match(
                    "/groups/members/search",
                    respAdmins.find(
                        "div:nth-child(1)>div:nth-child(1) a:not(.touchable)", first=True
                    ).attrs.get('href'),
                ):
                    admins = respAdmins.find("div:nth-of-type(2)>div.touchable a:not(.touchable)")
                else:
                    admins = respAdmins.find("div:first-child>div.touchable a:not(.touchable)")
                result["admins"] = [
                    {
                        "name": e.text,
                        "link": utils.filter_query_params(e.attrs["href"], blacklist=["refid"]),
                    }
                    for e in admins
                ]

            url = resp.find("a[href*='listType=list_nonfriend_nonadmin']", first=True)
            if kwargs.get("members", True):
                if url:
                    url = url.attrs["href"]
                    members = []
                    while url:
                        logger.debug(f"Requesting page from: {url}")
                        resp = self.get(url).html
                        elems = resp.find("#root div.touchable a:not(.touchable)")
                        members.extend([{"name": e.text, "link": e.attrs["href"]} for e in elems])
                        more = re.search(r'"m_more_item",href:"([^"]+)', resp.text)
                        if more:
                            url = more.group(1)
                        else:
                            url = None
                    result["other_members"] = [m for m in members if m not in result["admins"]]
                else:
                    logger.warning("No other members listed")
        except exceptions.LoginRequired as e:
            pass
        return result

    def get_shop(self, page, **kwargs) -> Iterator[Post]:
        self.set_user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8"
        )
        self.set_noscript(True)
        url = f"{page}/shop/"
        logger.debug(f"Fetching {url}")
        resp = self.get(url)
        more_links = resp.html.find("a[href]", containing="See More")
        if more_links:
            url = more_links[-1].attrs["href"]
            logger.debug(f"Fetching {url}")
            resp = self.get(url)
        items = resp.html.find("div.be")
        results = []
        for item in items:
            link_elem = item.find("div.bl a", first=True)
            name = link_elem.text
            link = link_elem.attrs["href"]
            image = item.find("img", first=True).attrs["src"]
            price = item.find("div.bl")[-1].text
            result = {"name": name, "link": link, "image": image, "price": price}
            results.append(result)
        return results

    def get_group_posts(self, group: Union[str, int], **kwargs) -> Iterator[Post]:
        self.set_user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8"
        )
        iter_pages_fn = partial(iter_group_pages, group=group, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_group_post, iter_pages_fn, **kwargs)

    def check_locale(self, response):
        if self.have_checked_locale:
            return
        match = re.search(r'"IntlCurrentLocale",\[\],{code:"(\w{2}_\w{2})"}', response.text)
        if match:
            locale = match.groups(1)[0]
            if locale != "en_US":
                warnings.warn(
                    f"Facebook language detected as {locale} - for best results, set to en_US"
                )
            self.have_checked_locale = True

    def get(self, url, **kwargs):
        try:
            self.request_count += 1
            url = str(url)
            if not url.startswith("http"):
                url = utils.urljoin(FB_MOBILE_BASE_URL, url)

            if kwargs.get("post"):
                kwargs.pop("post")
                response = self.session.post(url=url, **kwargs)
            else:
                response = self.session.get(url=url, **self.requests_kwargs, **kwargs)
            DEBUG = False
            if DEBUG:
                for filename in os.listdir("."):
                    if filename.endswith(".html") and filename.replace(".html", "") in url:
                        logger.debug(f"Replacing {url} content with {filename}")
                        with open(filename) as f:
                            response.html.html = f.read()
            response.html.html = response.html.html.replace('<!--', '').replace('-->', '')
            response.raise_for_status()
            self.check_locale(response)

            # Special handling for video posts that redirect to /watch/
            if response.url == "https://m.facebook.com/watch/?ref=watch_permalink":
                post_url = re.search("\d+", url).group()
                if post_url:
                    url = utils.urljoin(
                        FB_MOBILE_BASE_URL,
                        f"story.php?story_fbid={post_url}&id=1&m_entstream_source=timeline",
                    )
                    post = {"original_request_url": post_url, "post_url": url}
                    logger.debug(f"Requesting page from: {url}")
                    response = self.get(url)
            if "/watch/" in response.url:
                video_id = parse_qs(urlparse(response.url).query).get("v")[0]
                url = f"story.php?story_fbid={video_id}&id={video_id}&m_entstream_source=video_home&player_suborigin=entry_point&player_format=permalink"
                logger.debug(f"Fetching {url}")
                response = self.get(url)

            if "cookie/consent-page" in response.url:
                response = self.submit_form(response)
            if (
                response.url.startswith(FB_MOBILE_BASE_URL)
                and not response.html.find("script", first=True)
                and "script" not in response.html.html
                and self.session.cookies.get("noscript") != "1"
            ):
                warnings.warn(
                    f"Facebook served mbasic/noscript content unexpectedly on {response.url}"
                )
            if response.html.find("h1,h2", containing="Unsupported Browser"):
                warnings.warn(f"Facebook says 'Unsupported Browser'")
            title = response.html.find("title", first=True)
            not_found_titles = ["page not found", "content not found"]
            temp_ban_titles = [
                "you can't use this feature at the moment",
                "you can't use this feature right now",
                "you’re temporarily blocked",
            ]
            if "checkpoint" in response.url:
                if response.html.find("h1", containing="We suspended your account"):
                    raise exceptions.AccountDisabled("Your Account Has Been Disabled")
            if title:
                if title.text.lower() in not_found_titles:
                    raise exceptions.NotFound(title.text)
                elif title.text.lower() == "error":
                    raise exceptions.UnexpectedResponse("Your request couldn't be processed")
                elif title.text.lower() in temp_ban_titles:
                    raise exceptions.TemporarilyBanned(title.text)
                elif ">your account has been disabled<" in response.html.html.lower():
                    raise exceptions.AccountDisabled("Your Account Has Been Disabled")
                elif (
                    ">We saw unusual activity on your account. This may mean that someone has used your account without your knowledge.<"
                    in response.html.html
                ):
                    raise exceptions.AccountDisabled("Your Account Has Been Locked")
                elif (
                    title.text == "Log in to Facebook | Facebook"
                    or response.url.startswith(utils.urljoin(FB_MOBILE_BASE_URL, "login"))
                    or response.url.startswith(utils.urljoin(FB_W3_BASE_URL, "login"))
                ):
                    raise exceptions.LoginRequired(
                        "A login (cookies) is required to see this page"
                    )
            return response
        except RequestException as ex:
            logger.exception("Exception while requesting URL: %s\nException: %r", url, ex)
            raise

    def submit_form(self, response, extra_data={}):
        action = response.html.find("form", first=True).attrs.get('action')
        url = utils.urljoin(self.base_url, action)
        elems = response.html.find("input[name][value]")
        data = {elem.attrs['name']: elem.attrs['value'] for elem in elems}
        data.update(extra_data)
        response = self.session.post(url, data=data, **self.requests_kwargs)
        return response

    def login(self, email: str, password: str):
        response = self.get(self.base_url)

        datr_cookie = re.search('(?<=_js_datr",")[^"]+', response.html.html)
        if datr_cookie:
            cookie_value = datr_cookie.group()
            self.session.cookies.set('datr', cookie_value)

        response = self.submit_form(
            response, {"email": email, "pass": password, "_fb_noscript": None}
        )

        login_error = response.html.find('#login_error', first=True)
        if login_error:
            raise exceptions.LoginError(login_error.text)

        if "enter login code to continue" in response.text.lower():
            token = input("Enter 2FA token: ")
            response = self.submit_form(response, {"approvals_code": token})
            strong = response.html.find("strong", first=True)
            if strong and strong.text.startswith("The login code you entered doesn't match"):
                raise exceptions.LoginError(strong.text)
            # Remember Browser
            response = self.submit_form(response, {"name_action_selected": "save_device"})
            if "review recent login" in response.text.lower():
                response = self.submit_form(response)
                # Login near {location} from {browser} on {OS} ({time}). Unset "This wasn't me", leaving "This was me" set.
                response = self.submit_form(response, {"submit[This wasn't me]": None})
                # Remember Browser. Please save the browser that you just verified. You won't have to enter a code when you log in from browsers that you've saved.
                response = self.submit_form(response, {"name_action_selected": "save_device"})

        if "login approval needed" in response.text.lower() or "checkpoint" in response.url:
            input(
                "Login approval needed. From a browser logged into this account, approve this login from your notifications. Press enter once you've approved it."
            )
            response = self.submit_form(response, {"submit[Continue]": "Continue"})
        if "the password that you entered is incorrect" in response.text.lower():
            raise exceptions.LoginError("The password that you entered is incorrect")
        if 'c_user' not in self.session.cookies:
            with open("login_error.html", "w") as f:
                f.write(response.text)
            raise exceptions.LoginError("Login unsuccessful")

    def is_logged_in(self) -> bool:
        try:
            self.get('https://facebook.com/settings')
            return True
        except exceptions.LoginRequired:
            return False

    def _generic_get_posts(
        self,
        extract_post_fn,
        iter_pages_fn,
        page_limit=DEFAULT_PAGE_LIMIT,
        options=None,
        remove_source=True,
        latest_date=None,
        max_past_limit=5,
        **kwargs,
    ):

        if options is None:
            options = {}
        elif isinstance(options, set):
            warnings.warn("The options argument should be a dictionary.", stacklevel=3)
            options = {k: True for k in options}
        if self.session.cookies.get("noscript") == "1":
            options["noscript"] = True

        if page_limit and page_limit <= 2:
            warnings.warn(
                "A low page limit (<=2) might return no results, try increasing the limit",
                stacklevel=3,
            )

        # if latest_date is specified, iterate until the date is reached n times in a row (recurrent_past_posts)
        if latest_date is not None:

            # Pinned posts repeat themselves over time, so ignore them
            pinned_posts = []

            # Stats
            null_date_posts = 0
            total_scraped_posts = 0

            # Helpers
            recurrent_past_posts = 0
            show_every = 50
            done = False

            for page in iter_pages_fn():

                for post_element in page:
                    try:
                        post = extract_post_fn(post_element, options=options, request_fn=self.get)

                        if remove_source:
                            post.pop("source", None)

                        # date is None, no way to check latest_date, yield it
                        if post["time"] is None:
                            null_date_posts += 1

                        # date is above latest_date, yield it
                        if post["time"] > latest_date:
                            recurrent_past_posts = 0

                        # if any of above, yield the post and continue
                        if post["time"] is None or post["time"] > latest_date:
                            total_scraped_posts += 1
                            if total_scraped_posts % show_every == 0:
                                logger.info("Posts scraped: %s", total_scraped_posts)

                            yield post
                            continue

                        # else, the date is behind the date limit
                        recurrent_past_posts += 1

                        # and it has reached the max_past_limit posts
                        if recurrent_past_posts >= max_past_limit:
                            done = True
                            logger.info(
                                "Sequential posts behind latest_date reached. Stopping scraping."
                            )
                            logger.info(
                                "Posts with null date: %s",
                                null_date_posts,
                            )
                            break

                        # or the text is not banned (repeated)
                        if post["text"] is not None and post["text"] not in pinned_posts:
                            pinned_posts.append(post["text"])
                            logger.warning(
                                "Sequential post #%s behind the date limit: %s. Ignored (in logs) from now on.",
                                recurrent_past_posts,
                                post["time"],
                            )

                    except Exception as e:
                        logger.exception(
                            "An exception has occured during scraping: %s. Omitting the post...",
                            e,
                        )

                # if max_past_limit, stop
                if done:
                    break

        # else, iterate over pages as usual
        else:
            counter = itertools.count(0) if page_limit is None else range(page_limit)

            logger.debug("Starting to iterate pages")
            for i, page in zip(counter, iter_pages_fn()):
                logger.debug("Extracting posts from page %s", i)
                for post_element in page:
                    post = extract_post_fn(post_element, options=options, request_fn=self.get)
                    if remove_source:
                        post.pop('source', None)
                    yield post

    def get_groups_by_search(self, word: str, **kwargs):
        group_search_url = utils.urljoin(FB_MOBILE_BASE_URL, f"search/groups/?q={word}")
        r = self.get(group_search_url)
        for group_element in r.html.find('div[role="button"]'):
            button_id = group_element.attrs["id"]
            group_id = self.find_group_id(button_id, r.text)
            try:
                yield self.get_group_info(group_id)
            except AttributeError:
                continue

    @staticmethod
    def find_group_id(button_id, raw_html):
        """Each group button has an id, which appears later in the script
        tag followed by the group id."""
        s = raw_html[raw_html.rfind(button_id) :]
        group_id = s[s.find("result_id:") :].split(",")[0].split(":")[1]
        return int(group_id)
