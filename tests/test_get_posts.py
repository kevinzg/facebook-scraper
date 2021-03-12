import datetime

import pytest

from facebook_scraper import get_posts


@pytest.mark.vcr()
class TestGetPosts:
    @pytest.mark.skip(reason="This test uses different endpoint (/posts/)")
    def test_get_posts(self):
        expected_post = {
            'comments': 73,
            'image': 'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_oc=AQmzJTxqWcBz-Q2u7AX_Aj_6bwv6V86hZS-v9BY-3w0h7jy9_LGi-LXss6UJuQn9xhk&_nc_ht=scontent.faqp2-3.fna&_nc_tp=14&oh=a057d46d536592575cce1605eac62dc4&oe=5EE011FB',
            'images': [
                'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_oc=AQmzJTxqWcBz-Q2u7AX_Aj_6bwv6V86hZS-v9BY-3w0h7jy9_LGi-LXss6UJuQn9xhk&_nc_ht=scontent.faqp2-3.fna&_nc_tp=14&oh=a057d46d536592575cce1605eac62dc4&oe=5EE011FB',
                'https://scontent.faqp2-2.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96657922_3065146630236436_9052202957155598336_o.jpg?_nc_cat=101&_nc_sid=8024bb&_nc_ohc=HJe4yM4ZM-IAX_A4Gbb&_nc_ht=scontent.faqp2-2.fna&tp=14&oh=0f88fe17a844510b3ca40ecd53392657&oe=5FA220AD',
                'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96557798_3065146790236420_838564679184809984_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_ohc=ZAWOX3v_GjwAX_nMJvh&_nc_ht=scontent.faqp2-3.fna&tp=14&oh=0351cb4b748dd6ce296dd02341f3f949&oe=5FA16534',
                'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96688092_3065146896903076_7861539131082407936_o.jpg?_nc_cat=108&_nc_sid=8024bb&_nc_ohc=G3b4bTeYIoEAX8IYjU4&_nc_ht=scontent.faqp2-3.fna&tp=14&oh=ae53e61554bfe97b85fe3dff884a4a2f&oe=5FA1DB01',
            ],
            'video': None,
            'video_thumbnail': None,
            'likes': 1334,
            'link': 'https://www.nintendo.com/wallpapers/',
            'post_id': '3065154550235644',
            'post_text': 'Check out these themed wallpapers and many more at the link '
            'below for your personal use! We hope you enjoy them!\n\n'
            'https://www.nintendo.com/wallpapers/',
            'post_url': 'https://facebook.com/Nintendo/posts/3065154550235644',
            'shared_text': '',
            'shares': 0,
            'text': 'Check out these themed wallpapers and many more at the link below '
            'for your personal use! We hope you enjoy them!\n\n'
            'https://www.nintendo.com/wallpapers/',
            'time': datetime.datetime(2020, 5, 12, 20, 1, 18),
            'user_id': '119240841493711',
            'username': 'Nintendo',
            'video_id': None,
            'is_live': False,
        }

        post = next(get_posts(account='Nintendo'))

        assert post == expected_post

    @pytest.mark.skip(reason="Test data needs to be updated")
    def test_get_posts_with_extra_info(self):
        expected_post = {
            'comments': 111,
            'fetched_time': datetime.datetime(2020, 5, 13, 16, 14, 4, 993758),
            'image': 'https://scontent.faqp2-3.fna.fbcdn.net/v/t1.0-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_o.jpg?_nc_cat=103&_nc_sid=8024bb&_nc_oc=AQmzJTxqWcBz-Q2u7AX_Aj_6bwv6V86hZS-v9BY-3w0h7jy9_LGi-LXss6UJuQn9xhk&_nc_ht=scontent.faqp2-3.fna&_nc_tp=14&oh=a057d46d536592575cce1605eac62dc4&oe=5EE011FB',
            'video': None,
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

    @pytest.mark.skip(reason="This test uses different endpoint (/posts/)")
    def test_get_posts_fields_presence(self):
        posts = list(get_posts(account='Nintendo', pages=2, extra_info=True))

        assert len(posts) == 6

        for post in posts:
            assert 'post_id' in post
            assert 'text' in post
            assert 'time' in post
            assert 'image' in post
            assert 'video' in post
            assert 'likes' in post
            assert 'comments' in post
            assert 'shares' in post
            assert 'post_url' in post
            assert 'link' in post

    @pytest.mark.skip(reason="Test data needs to be updated")
    def test_get_posts_with_extra_info_fields_presence(self):
        posts = list(get_posts(account='Nintendo', pages=2, extra_info=True))

        assert len(posts) == 6

        for post in posts:
            assert 'post_id' in post
            assert 'text' in post
            assert 'time' in post
            assert 'video' in post
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


@pytest.mark.vcr()
class TestGetGroupPosts:
    @pytest.mark.skip(reason="Post schema is not stable")
    def test_get_group_posts(self):
        text = (
            'Hola!, This group is aimed to create opportunities for South '
            'American students in Computer Science and related fields.\n\n'
            'Hope this will help us to know what we are doing in our work, '
            'achievements to be recognized, increase fairness in our area, and '
            'maybe conferences where we might meet.\n\n'
            'Professors and professionals are also welcomed to share their '
            'experiences and to collaborate among us and learn together.\n\n'
            'Some short rules for a happy co-existence:\n'
            '1. No business advertisement or spam.\n'
            '2. Topics relevant to Computing, Computer Science, Software '
            'Engineering, and Education.\n'
            '3. Political and religious advertisement are not allowed.'
        )
        expected_post = {
            'comments': 1,
            'image': None,
            'images': [],
            'video_id': None,
            'video': None,
            'video_thumbnail': None,
            'video_id': None,
            'likes': 26,
            'link': None,
            'post_id': None,
            'post_text': text,
            'post_url': None,
            'user_id': 757122227,
            'username': 'Omar U. Florez',
            'shared_text': '',
            'shares': 0,
            'text': text,
            'time': datetime.datetime(2018, 4, 3, 20, 2, 0),
            'is_live': False,
            'factcheck': None,
        }

        post = next(get_posts(group=117507531664134))

        assert post == expected_post
