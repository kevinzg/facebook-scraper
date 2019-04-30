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
 'likes': 2036,
 'comments': 214,
 'shares': 0,
 'post_url': 'https://m.facebook.com/story.php'
             '?story_fbid=2257188721032235&id=119240841493711'}
```

### Notes

- There is no guarantee that every field will be extracted (they might be `None`).
- Shares doesn't seem to work at the moment.
