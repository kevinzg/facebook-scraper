import itertools
import logging
from urllib.parse import urljoin
import warnings
import re
from functools import partial
from typing import Iterator, Union
import json
from urllib.parse import parse_qs, urlparse

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
from .extractors import extract_group_post, extract_post, extract_photo_post, PostExtractor
from .fb_types import Post, Profile
from .page_iterators import iter_group_pages, iter_pages, iter_photos
from . import exceptions


logger = logging.getLogger(__name__)


class FacebookScraper:
    """Class for creating FacebookScraper Iterators"""

    base_url = FB_MOBILE_BASE_URL
    default_headers = {
        'Accept-Language': 'en-US,en;q=0.5',
        "Sec-Fetch-User": "?1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36",
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

    def set_user_agent(self, user_agent):
        self.session.headers["User-Agent"] = user_agent

    def set_noscript(self, noscript):
        if noscript:
            self.session.cookies.set("noscript", "1")
        else:
            self.session.cookies.set("noscript", "0")

    def set_proxy(self, proxy):
        self.requests_kwargs.update({'proxies': {'http': proxy, 'https': proxy}})
        ip = self.get(
            "http://lumtest.com/myip.json", headers={"Accept": "application/json"}
        ).json()
        logger.debug(f"Proxy details: {ip}")

    def get_posts(self, account: str, **kwargs) -> Iterator[Post]:
        kwargs["scraper"] = self
        iter_pages_fn = partial(iter_pages, account=account, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_photos(self, account: str, **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_photos, account=account, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_post, iter_pages_fn, **kwargs)

    def get_posts_by_url(self, post_urls, options={}, remove_source=True) -> Iterator[Post]:
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
            if "/watch/" in response.url:
                video_id = parse_qs(urlparse(response.url).query).get("v")[0]
                response = self.get(video_id)
            elem = response.html.find('[data-ft*="top_level_post_id"]', first=True)
            photo_post = False
            if response.html.find("div.msg", first=True):
                photo_post = True
                elem = response.html.find("#root", first=True)
            if not elem:
                logger.warning("No raw posts (<article> elements) were found in this page.")
            else:
                comments_area = response.html.find('div.ufi', first=True)
                if comments_area:
                    # Makes likes/shares regexes work
                    elem = utils.make_html_element(
                        elem.html.replace("</footer>", comments_area.html + "</footer>")
                    )

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
            elems = response.html.find('div[data-sigil="undoable-action"]')
            logger.debug(f"Found {len(elems)} friends")
            for elem in elems:
                name = elem.find("h3>a", first=True)
                tagline = elem.find("div.notice.ellipsis", first=True).text
                profile_picture = elem.find("i.profpic", first=True).attrs.get("style")
                match = re.search(r"url\('(.+)'\)", profile_picture)
                if match:
                    profile_picture = utils.decode_css_url(match.groups()[0])
                user_id = json.loads(
                    elem.find("a.touchable[data-store]", first=True).attrs["data-store"]
                ).get("id")
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
            more = re.search(r'href:"(/[^/]+/friends[^"]+)"', response.text)
            if more:
                friend_url = utils.urljoin(FB_MOBILE_BASE_URL, more.group(1))
                if request_url_callback:
                    request_url_callback(friend_url)
            else:
                return

    def get_profile(self, account, **kwargs) -> Profile:
        result = {}

        if kwargs.get("allow_extra_requests", True):
            logger.debug(f"Requesting page from: {account}")
            response = self.get(account)
            photo_links = response.html.find("a[href^='/photo.php']")
            if photo_links:
                cover_photo = photo_links[0]
                result["cover_photo_text"] = cover_photo.attrs.get("title")
                response = self.get(cover_photo.attrs.get("href"))
                extractor = PostExtractor(response.html, kwargs, self.get)
                result["cover_photo"] = extractor.extract_photo_link_HQ(response.html.html)

                profile_photo = photo_links[1]
                response = self.get(profile_photo.attrs.get("href"))
                result["profile_picture"] = extractor.extract_photo_link_HQ(response.html.html)
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
        match = re.search(r'entity_id:(\d+),', response.html.html)
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
                    header in ["Contact Info", "Basic info", "Other names"] and len(bits) % 2 == 0
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
        return result

    def get_page_info(self, page, **kwargs) -> Profile:
        result = {}
        for post in self.get_posts(page, **kwargs):
            logger.debug(f"Fetching {post['post_id']}")
            resp = self.get(post["post_id"])
            elem = resp.html.find("script[type='application/ld+json']", first=True)
            if not elem:
                continue
            meta = json.loads(elem.text)
            if meta.get("creator"):
                result = meta["creator"]
                result["type"] = result.pop("@type")
                desc = resp.html.find("meta[name='description']", first=True)
                if desc:
                    match = re.search(r'(\d[\d,.]+)', desc.attrs["content"])
                    if match:
                        result["likes"] = utils.parse_int(match.groups()[0])
                try:
                    for interaction in result.get("interactionStatistic", []):
                        if interaction["interactionType"] == {
                            "@type": "http://schema.org/FollowAction"
                        }:
                            result["followers"] = interaction["userInteractionCount"]
                except TypeError as e:
                    logger.error(e)
                result.pop("interactionStatistic", None)
                break

        try:
            about_url = f'/{page}/about/'
            logger.debug(f"Requesting page from: {about_url}")
            resp = self.get(about_url)
            desc = resp.html.find("meta[name='description']", first=True)
            if desc:
                logger.debug(desc.attrs["content"])
                match = re.search(r'(\d[\d,.]+)', desc.attrs["content"])
                if match:
                    result["likes"] = utils.parse_int(match.groups()[0])
            result["about"] = resp.html.find('#pages_msite_body_contents', first=True).text
        except Exception as e:
            logger.error(e)
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
        url = members.find("a", first=True).attrs.get("href")
        logger.debug(f"Requesting page from: {url}")
        try:
            resp = self.get(url).html
            admins = resp.find("div:first-child>div.touchable a:not(.touchable)")
            result["admins"] = [
                {
                    "name": e.text,
                    "link": utils.filter_query_params(e.attrs["href"], blacklist=["refid"]),
                }
                for e in admins
            ]
            url = resp.find("a[href^='/browse/group/members']", first=True)
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
        url = more_links[-1].attrs["href"]
        logger.debug(f"Fetching {url}")
        resp = self.get(url)
        items = resp.html.find("div.be")
        results = []
        for item in items:
            link_elem = item.find("div.bk.bl a", first=True)
            name = link_elem.text
            link = link_elem.attrs["href"]
            image = item.find("img", first=True).attrs["src"]
            price = item.find("div.bk.bl")[-1].text
            result = {"name": name, "link": link, "image": image, "price": price}
            results.append(result)
        return results

    def get_group_posts(self, group: Union[str, int], **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_group_pages, group=group, request_fn=self.get, **kwargs)
        return self._generic_get_posts(extract_group_post, iter_pages_fn, **kwargs)

    def check_locale(self, response):
        if self.have_checked_locale:
            return
        match = re.search(r'"IntlCurrentLocale",\[\],{code:"(\w{2}_\w{2})"}', response.text)
        if match:
            locale = match.groups(1)[0]
            if locale != "en_US":
                warnings.warn(f"Locale detected as {locale} - for best results, set to en_US")
            self.have_checked_locale = True

    def get(self, url, **kwargs):
        try:
            if not url.startswith("http"):
                url = utils.urljoin(FB_MOBILE_BASE_URL, url)
            response = self.session.get(url=url, **self.requests_kwargs, **kwargs)
            response.html.html = response.html.html.replace('<!--', '').replace('-->', '')
            response.raise_for_status()
            self.check_locale(response)
            if "cookie/consent-page" in response.url:
                response = self.submit_form(response)
            if (
                response.url.startswith(FB_MOBILE_BASE_URL)
                and not response.html.find("script", first=True)
                and self.session.cookies.get("noscript") != "1"
            ):
                warnings.warn(
                    f"Facebook served mbasic/noscript content unexpectedly on {response.url}"
                )
            title = response.html.find("title", first=True)
            not_found_titles = ["page not found", "content not found"]
            temp_ban_titles = [
                "you can't use this feature at the moment",
                "you can't use this feature right now",
                "you’re temporarily blocked",
            ]
            if title:
                if title.text.lower() in not_found_titles:
                    raise exceptions.NotFound(title.text)
                elif title.text.lower() == "error":
                    raise exceptions.UnexpectedResponse("Your request couldn't be processed")
                elif title.text.lower() in temp_ban_titles:
                    raise exceptions.TemporarilyBanned(title.text)
                elif ">Your Account Has Been Disabled<" in response.html.html:
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
                    or (
                        ", log in to Facebook." in response.text
                        and not response.html.find(
                            "article[data-ft],div.async_like[data-ft],div.msg"
                        )
                    )
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
        response = self.session.post(url, data=data)
        return response

    def login(self, email: str, password: str):
        response = self.get(self.base_url)
        response = self.submit_form(
            response, {"email": email, "pass": password, "_fb_noscript": None}
        )

        login_error = response.html.find('#login_error', first=True)
        if login_error:
            raise exceptions.LoginError(login_error.text)

        if "Enter login code to continue" in response.text:
            token = input("Enter 2FA token: ")
            response = self.submit_form(response, {"approvals_code": token})
            strong = response.html.find("strong", first=True)
            if strong and strong.text.startswith("The login code you entered doesn't match"):
                raise exceptions.LoginError(strong.text)
            # Remember Browser
            response = self.submit_form(response, {"name_action_selected": "save_device"})
            if "Review recent login" in response.text:
                response = self.submit_form(response)
                # Login near {location} from {browser} on {OS} ({time}). Unset "This wasn't me", leaving "This was me" set.
                response = self.submit_form(response, {"submit[This wasn't me]": None})
                # Remember Browser. Please save the browser that you just verified. You won't have to enter a code when you log in from browsers that you've saved.
                response = self.submit_form(response, {"name_action_selected": "save_device"})

        if "Login approval needed" in response.text or "checkpoint" in response.url:
            raise exceptions.LoginError("Login approval needed")
        if 'c_user' not in self.session.cookies:
            raise exceptions.LoginError("Login unsuccessful")

    def is_logged_in(self) -> bool:
        try:
            self.get('https://m.facebook.com/settings')
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
        **kwargs,
    ):
        counter = itertools.count(0) if page_limit is None else range(page_limit)

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

        logger.debug("Starting to iterate pages")
        for i, page in zip(counter, iter_pages_fn()):
            logger.debug("Extracting posts from page %s", i)
            for post_element in page:
                post = extract_post_fn(post_element, options=options, request_fn=self.get)
                if remove_source:
                    post.pop('source', None)
                yield post
