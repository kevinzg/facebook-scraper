import itertools
import logging
from urllib.parse import urljoin
import warnings
import re
from functools import partial
from typing import Iterator, Union

from requests import RequestException
from requests_html import HTMLSession

from . import utils
from .constants import DEFAULT_PAGE_LIMIT, FB_BASE_URL, FB_MOBILE_BASE_URL, FB_W3_BASE_URL
from .extractors import extract_group_post, extract_post, extract_photo_post
from .fb_types import Post, Profile
from .page_iterators import iter_group_pages, iter_pages
from . import exceptions


logger = logging.getLogger(__name__)


class FacebookScraper:
    """Class for creating FacebookScraper Iterators"""

    base_url = FB_MOBILE_BASE_URL
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/76.0.3809.87 Safari/537.36"
    )
    default_headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.5',
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

    def set_proxy(self, proxy):
        self.requests_kwargs.update({
            'proxies': {
                'http': proxy,
                'https': proxy
            }
        })
        ip = self.get("http://ifconfig.co", headers={"Accept": "application/json"}).json()
        logger.debug(f"Proxy details: {ip}")

    def get_posts(self, account: str, **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_pages, account=account, request_fn=self.get, **kwargs)
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
            post = {
                "original_request_url": post_url,
                "post_url": url
            }
            logger.debug(f"Requesting page from: {url}")
            response = self.get(url)
            elem = response.html.find('article[data-ft],div.async_like[data-ft]', first=True)
            photo_post = False
            if response.html.find("div.msg", first=True):
                photo_post = True
                elem = response.html
            if not elem:
                logger.warning("No raw posts (<article> elements) were found in this page.")
            else:
                comments_area = response.html.find('div[data-sigil="m-mentions-expand"]', first=True)
                if comments_area:
                    # Makes likes/shares regexes work
                    elem = utils.make_html_element(elem.html.replace("</footer>", comments_area.html + "</footer>"))

                if photo_post:
                    post.update(extract_photo_post(elem, request_fn=self.get, options=options))
                elif url.startswith(utils.urljoin(FB_MOBILE_BASE_URL, "/groups/")):
                    post.update(extract_group_post(elem, request_fn=self.get, options=options))
                else:
                    post.update(extract_post(elem, request_fn=self.get, options=options))
                if not post.get("post_url"):
                    post["post_url"] = url
                if remove_source:
                    post.pop('source', None)
            yield post

    def get_profile(self, account, **kwargs) -> Profile:
        about_url = utils.urljoin(FB_MOBILE_BASE_URL, f'/{account}/about/')
        logger.debug(f"Requesting page from: {about_url}")
        response = self.get(about_url)
        result = {}
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
                header = "About" # Truncate strings like "About Mark"
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
                bits = card.text.split("\n")[1:] # Remove header
                if len(bits) >= 3 and header == "Relationship":
                    result[header] = {
                        "to": bits[0],
                        "type": bits[1],
                        "since": bits[2]
                    }
                elif len(bits) == 1:
                    result[header] = bits[0]
                elif header in ["Contact Info", "Basic info", "Other names"] and len(bits) % 2 == 0: # Divisible by two, assume pairs
                    pairs = {}
                    for i in range(0, len(bits), 2):
                        pairs[bits[i + 1]] = bits[i]
                    result[header] = pairs
                else:
                    result[header] = "\n".join(bits)
        friend_opt = kwargs.get("friends")
        if friend_opt:
            limit = None
            if type(friend_opt) in [int, float]:
                limit = friend_opt
            friend_url = utils.urljoin(FB_MOBILE_BASE_URL, f'/{account}/friends/')
            elems = []
            while friend_url:
                logger.debug(f"Requesting page from: {friend_url}")
                response = self.get(friend_url)
                elems.extend(response.html.find('div[data-sigil="undoable-action"]'))
                if limit and len(elems) > limit:
                    break
                more = re.search(r'href:"(/[^/]+/friends[^"]+)"', response.text)
                if more:
                    friend_url = utils.urljoin(FB_MOBILE_BASE_URL, more.group(1))
                else:
                    break
            logger.debug(f"Found {len(elems)} friends")
            friends = []
            for elem in elems:
                name = elem.find("h3>a", first=True)
                tagline = elem.find("div.notice.ellipsis", first=True).text
                friends.append({
                    "link": name.attrs.get("href"),
                    "name": name.text,
                    "tagline": tagline
                })
            result["Friends"] = friends
        return result

    def get_group_info(self, group, **kwargs) -> Profile:
        url = f'/groups/{group}'
        logger.debug(f"Requesting page from: {url}")
        resp = self.get(url).html
        url = resp.find("a[href*='?view=info']", first=True).attrs["href"]
        logger.debug(f"Requesting page from: {url}")
        resp = self.get(url).html
        result = {}
        result["id"] = re.search(r'/groups/(\d+)', url).group(1)
        result["name"] = resp.find("header h3", first=True).text
        result["type"] = resp.find("header div", first=True).text
        members = resp.find("div[data-testid='m_group_sections_members']", first=True)
        result["members"] = utils.parse_int(members.text)
        url = members.find("a", first=True).attrs.get("href")
        logger.debug(f"Requesting page from: {url}")
        try:
            resp = self.get(url).html
            admins = resp.find("div:first-child>div.touchable a:not(.touchable)")
            result["admins"] = [{"name": e.text, "link": e.attrs["href"]} for e in admins]
        except Exception as e:
            pass
        return result

    def get_group_posts(self, group: Union[str, int], **kwargs) -> Iterator[Post]:
        iter_pages_fn = partial(iter_group_pages, group=group, request_fn=self.get)
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
            title = response.html.find("title", first=True)
            not_found_titles = ["page not found", "content not found"]
            temp_ban_titles = ["you can't use this feature at the moment", "you’re temporarily blocked"]
            if title:
                if title.text.lower() in not_found_titles:
                    raise exceptions.NotFound(title.text)
                elif title.text.lower() in temp_ban_titles:
                    raise exceptions.TemporarilyBanned(title.text)
                elif ">Your Account Has Been Disabled<" in response.html.html:
                    raise exceptions.AccountDisabled("Your Account Has Been Disabled")
                elif title.text == "Log in to Facebook | Facebook" or response.url.startswith(utils.urljoin(FB_MOBILE_BASE_URL, "login")):
                    raise exceptions.LoginRequired("A login (cookies) is required to see this page")
            return response
        except RequestException as ex:
            logger.exception("Exception while requesting URL: %s\nException: %r", url, ex)
            raise

    def login(self, email: str, password: str):
        login_page = self.get(self.base_url)
        login_action = login_page.html.find('#login_form', first=True).attrs.get('action')

        elems = login_page.html.find('#login_form > input[name][value]')
        data = { elem.attrs['name']: elem.attrs['value'] for elem in elems }
        data["email"] = email
        data["pass"] = password

        response = self.session.post(
            utils.urljoin(self.base_url, login_action), data=data
        )
        response_text = response.html.find('#viewport', first=True).text

        logger.debug("Login response text: %s", response_text)

        login_error = response.html.find('#login_error', first=True)
        if login_error:
            logger.error("Login error: %s", login_error.text)

        if 'c_user' not in self.session.cookies:
            warnings.warn('login unsuccessful')

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
    ):
        counter = itertools.count(0) if page_limit is None else range(page_limit)

        if options is None:
            options = {}
        elif isinstance(options, set):
            warnings.warn("The options argument should be a dictionary.", stacklevel=3)
            options = {k: True for k in options}

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
