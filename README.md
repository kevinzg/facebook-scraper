# Facebook Scraper

Scrape Facebook public pages using the web API. Inspired by [twitter-scraper](https://github.com/kennethreitz/twitter-scraper).

## Usage

```python
>>> from facebook_scraper import get_posts

>>> for post in get_posts('nintendo', pages=1):
...     print(post['text'][:50])
...
The final step on the road to the Super Smash Bros
We’re headed to PAX East 3/28-3/31 with new games
```
