import datetime

import pytest
from facebook_scraper import get_posts


@pytest.mark.vcr()
class TestGetPosts:
    def test_get_posts(self):
        expected_post = {
            'comments': 73,
            'image': 'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_oc=AQmzJTxqWcBz-Q2u7AX_Aj_6bwv6V86hZS-v9BY-3w0h7jy9_LGi-LXss6UJuQn9xhk&_nc_ht=scontent.faqp2-3.fna&_nc_tp=14&oh=a057d46d536592575cce1605eac62dc4&oe=5EE011FB',
            'likes': 1334,
            'link': 'https://www.nintendo.com/wallpapers/',
            'post_id': '3065154550235644',
            'post_text': 'Check out these themed wallpapers and many more at the link '
            'below for your personal use! We hope you enjoy them!\n'
            'https://www.nintendo.com/wallpapers/',
            'post_url': 'https://m.facebook.com/story.php?story_fbid=3065154550235644&id=119240841493711',
            'shared_text': '',
            'shares': 0,
            'text': 'Check out these themed wallpapers and many more at the link below '
            'for your personal use! We hope you enjoy them!\n'
            'https://www.nintendo.com/wallpapers/',
            'time': datetime.datetime(2020, 5, 12, 20, 1, 18),
        }

        post = next(get_posts(account='Nintendo'))

        assert post == expected_post

    def test_get_posts_with_extra_info(self):
        expected_post = {
            'comments': 111,
            'fetched_time': datetime.datetime(2020, 5, 13, 16, 14, 4, 993758),
            'image': 'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_oc=AQmzJTxqWcBz-Q2u7AX_Aj_6bwv6V86hZS-v9BY-3w0h7jy9_LGi-LXss6UJuQn9xhk&_nc_ht=scontent.faqp2-3.fna&_nc_tp=14&oh=a057d46d536592575cce1605eac62dc4&oe=5EE011FB',
            'likes': 1754,
            'link': 'https://www.nintendo.com/wallpapers/',
            'post_id': '3065154550235644',
            'post_text': 'Check out these themed wallpapers and many more at the link '
            'below for your personal use! We hope you enjoy them!\n'
            'https://www.nintendo.com/wallpapers/',
            'post_url': 'https://m.facebook.com/story.php?story_fbid=3065154550235644&id=119240841493711',
            'reactions': {
                'anger': 2,
                'haha': 3,
                'like': 1334,
                'love': 321,
                'support': 77,
                'wow': 17,
            },
            'shared_text': '',
            'shares': 225,
            'text': 'Check out these themed wallpapers and many more at the link below '
            'for your personal use! We hope you enjoy them!\n'
            'https://www.nintendo.com/wallpapers/',
            'time': datetime.datetime(2020, 5, 12, 20, 1, 18),
            'w3_fb_url': 'https://www.facebook.com/Nintendo/posts/3065154550235644',
        }

        post = next(get_posts(account='Nintendo', extra_info=True))

        post.pop('fetched_time')  # Do not check this field
        expected_post.pop('fetched_time')

        assert post == expected_post

    def test_get_posts_fields_presence(self):
        posts = list(get_posts(account='Nintendo', pages=2, extra_info=True))

        assert len(posts) == 6

        for post in posts:
            assert 'post_id' in post
            assert 'text' in post
            assert 'time' in post
            assert 'image' in post
            assert 'likes' in post
            assert 'comments' in post
            assert 'shares' in post
            assert 'post_url' in post
            assert 'link' in post

    def test_get_posts_with_extra_info_fields_presence(self):
        posts = list(get_posts(account='Nintendo', pages=2, extra_info=True))

        assert len(posts) == 6

        for post in posts:
            assert 'post_id' in post
            assert 'text' in post
            assert 'time' in post
            assert 'image' in post
            assert 'likes' in post
            assert 'comments' in post
            assert 'shares' in post
            assert 'post_url' in post
            assert 'link' in post
            assert 'shares' in post
            assert 'likes' in post
            assert 'reactions' in post
            assert 'comments' in post
            assert 'w3_fb_url' in post
            assert 'fetched_time' in post
