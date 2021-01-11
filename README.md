# Facebook Scraper

Scrape Facebook public pages without an API key. Inspired by [twitter-scraper](https://github.com/kennethreitz/twitter-scraper).


## Install

```sh
pip install facebook-scraper
```


## Usage

Send the unique **page name** as the first parameter and you're good to go:

```python
>>> from facebook_scraper import get_posts

>>> for post in get_posts('nintendo', pages=1):
...     print(post['text'][:50])
...
The final step on the road to the Super Smash Bros
We’re headed to PAX East 3/28-3/31 with new games
```

### CLI usage
```sh
$ facebook-scraper --filename nintendo_page_posts.csv --pages 1 nintendo
```
Use
```sh
$ facebook-scraper --help
```
for more details on CLI usage.

Note: If you get a `UnicodeEncodeError` try adding `--encoding utf-8`.

### Optional parameters
*(For the `get_posts` function)*.

- **group**: group id, to scrape groups instead of pages. Default is `None`.
- **pages**: how many pages of posts to request, usually the first page has 2 posts and the rest 4. Default is 10.
- **timeout**: how many seconds to wait before timing out. Default is 5.
- **credentials**: tuple of user and password to login before requesting the posts. Default is `None`.
- **extra_info**: bool, if true the function will try to do an extra request to get the post reactions. Default is False.
- **youtube_dl**: bool, use Youtube-DL for (high-quality) video extraction. You need to have youtube-dl installed on your environment. Default is False.


## Post example

```python
{'post_id': '2257188721032235',
 'text': 'Don’t let this diminutive version of the Hero of Time fool you, '
         'Young Link is just as heroic as his fully grown version! Young Link '
         'joins the Super Smash Bros. series of amiibo figures!',
 'time': datetime.datetime(2019, 4, 29, 12, 0, 1),
 'image': 'https://scontent.flim16-1.fna.fbcdn.net'
          '/v/t1.0-0/cp0/e15/q65/p320x320'
          '/58680860_2257182054366235_1985558733786185728_n.jpg'
          '?_nc_cat=1&_nc_ht=scontent.flim16-1.fna'
          '&oh=31b0ba32ec7886e95a5478c479ba1d38&oe=5D6CDEE4',
 'images': ['https://scontent.flim16-1.fna.fbcdn.net'
          '/v/t1.0-0/cp0/e15/q65/p320x320'
          '/58680860_2257182054366235_1985558733786185728_n.jpg'
          '?_nc_cat=1&_nc_ht=scontent.flim16-1.fna'
          '&oh=31b0ba32ec7886e95a5478c479ba1d38&oe=5D6CDEE4'],
 'likes': 2036,
 'comments': 214,
 'shares': 0,
 'reactions': {'like': 135, 'love': 64, 'haha': 10, 'wow': 4, 'anger': 1},  # if `extra_info` was set
 'post_url': 'https://m.facebook.com/story.php'
             '?story_fbid=2257188721032235&id=119240841493711',
 'link': 'https://bit.ly/something', 
 'is_live': False}
```


### Notes

- There is no guarantee that every field will be extracted (they might be `None`).
- Shares doesn't seem to work at the moment.
- Group posts may be missing some fields like `time` and `post_url`.
- Group scraping may return only one page and not work on private groups.


## To-Do

- Async support
- Image galleries
- Profiles or post authors
- Comments


## Alternatives and related projects

- [facebook-post-scraper](https://github.com/brutalsavage/facebook-post-scraper). Has comments. Uses Selenium.
- [facebook-scraper-selenium](https://github.com/apurvmishra99/facebook-scraper-selenium). "Scrape posts from any group or user into a .csv file without needing to register for any API access".
- [Ultimate Facebook Scraper](https://github.com/harismuneer/Ultimate-Facebook-Scraper).  "Scrapes almost everything about a Facebook user's profile". Uses Selenium.
- [Unofficial APIs](https://github.com/Rolstenhouse/unofficial-apis). List of unofficial APIs for various services, none for Facebook for now, but might be worth to check in the future.
- [major-scrapy-spiders](https://github.com/talhashraf/major-scrapy-spiders). Has a profile spider for Scrapy.
- [facebook-page-post-scraper](https://github.com/minimaxir/facebook-page-post-scraper). Seems abandoned.
    - [FBLYZE](https://github.com/isaacmg/fb_scraper). Fork (?).
- [RSSHub](https://github.com/DIYgod/RSSHub/blob/master/lib/routes/facebook/page.js). Generates an RSS feed from Facebook pages.
- [RSS-Bridge](https://github.com/RSS-Bridge/rss-bridge/blob/master/bridges/FacebookBridge.php). Also generates RSS feeds from Facebook pages.
