import datetime

import pytest

from facebook_scraper import *


@pytest.mark.vcr()
class TestGetPosts:
    def test_get_posts(self):
        expected_post = {
            'available': True,
            'comments': 149,
            'comments_full': None,
            'factcheck': None,
            'image': 'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=d32fa3269feeaf6904d78a512f41ab26&oe=60E673C5',
            'image_id': '3065146500236449',
            'image_ids': [
                '3065146500236449',
                '3065146626903103',
                '3065146783569754',
                '3065146886903077',
            ],
            'image_lowquality': 'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/cp0/e15/q65/p720x720/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=3&oh=426e258c934177d9ded48435efaecc6c&oe=60E74054',
            'images': [
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=d32fa3269feeaf6904d78a512f41ab26&oe=60E673C5',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96657922_3065146630236436_9052202957155598336_n.jpg?_nc_cat=101&ccb=1-3&_nc_sid=8024bb&_nc_ohc=MwI_Au5sC60AX93Dkix&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=b947668e646a0e7614671deff90dc9a3&oe=60E41393',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96557798_3065146790236420_838564679184809984_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=ydkcrs8kPykAX_0Fdn4&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=7884c93d73b2a9f806baf829c8f941b0&oe=60E4D7FB',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96688092_3065146896903076_7861539131082407936_n.jpg?_nc_cat=108&ccb=1-3&_nc_sid=8024bb&_nc_ohc=vqgGsFXTmO4AX82bX5z&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=379eb1c4551d74a13a4cafb07524288e&oe=60E6753F',
            ],
            'images_description': [
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
            ],
            'images_lowquality': [
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/cp0/e15/q65/p720x720/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=3&oh=426e258c934177d9ded48435efaecc6c&oe=60E74054',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96657922_3065146630236436_9052202957155598336_n.jpg?_nc_cat=101&ccb=1-3&_nc_sid=8024bb&_nc_ohc=MwI_Au5sC60AX93Dkix&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=5c016bd47d3d9ab3ba997b48dbc21a97&oe=60E75F2D',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96557798_3065146790236420_838564679184809984_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&_nc_ohc=ydkcrs8kPykAX_0Fdn4&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=ca962fe95d846cbd6e4e78b0884572c9&oe=60E51308',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96688092_3065146896903076_7861539131082407936_n.jpg?_nc_cat=108&ccb=1-3&_nc_sid=8024bb&_nc_ohc=vqgGsFXTmO4AX82bX5z&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=7e9da116d24a9faee2fe15c16d7dea8f&oe=60E3DD81',
            ],
            'images_lowquality_description': [
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
            ],
            'is_live': False,
            'likes': 1615,
            'link': 'https://www.nintendo.com/wallpapers/',
            'original_request_url': 3065154550235644,
            'post_id': '3065154550235644',
            'post_text': 'Check out these themed wallpapers and many more at the link '
            'below for your personal use! We hope you enjoy them!\n'
            '\n'
            'https://www.nintendo.com/wallpapers/',
            'post_url': 'https://facebook.com/story.php?story_fbid=3065154550235644&id=119240841493711',
            'reaction_count': None,
            'reactions': None,
            'reactors': None,
            'shared_post_id': None,
            'shared_post_url': None,
            'shared_text': '',
            'shared_time': None,
            'shared_user_id': None,
            'shared_username': None,
            'shares': 281,
            'text': 'Check out these themed wallpapers and many more at the link below '
            'for your personal use! We hope you enjoy them!\n'
            '\n'
            'https://www.nintendo.com/wallpapers/',
            'time': datetime.datetime(2020, 5, 13, 13, 1, 18),
            'user_id': '119240841493711',
            'user_url': 'https://facebook.com/Nintendo/?refid=52&__tn__=C-R',
            'username': 'Nintendo',
            'video': None,
            'video_duration_seconds': None,
            'video_height': None,
            'video_id': None,
            'video_quality': None,
            'video_size_MB': None,
            'video_thumbnail': None,
            'video_watches': None,
            'video_width': None,
            'w3_fb_url': None,
        }

        post = next(get_posts(post_urls=[3065154550235644]))

        assert post == expected_post

    def test_get_posts_with_extra_info(self):
        expected_post = {
            'available': True,
            'comments': 149,
            'comments_full': None,
            'factcheck': None,
            'fetched_time': datetime.datetime(2021, 6, 9, 10, 31, 43, 834002),
            'image': 'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=d32fa3269feeaf6904d78a512f41ab26&oe=60E673C5',
            'image_id': '3065146500236449',
            'image_ids': [
                '3065146500236449',
                '3065146626903103',
                '3065146783569754',
                '3065146886903077',
            ],
            'image_lowquality': 'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/cp0/e15/q65/p720x720/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=3&oh=426e258c934177d9ded48435efaecc6c&oe=60E74054',
            'images': [
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=d32fa3269feeaf6904d78a512f41ab26&oe=60E673C5',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96657922_3065146630236436_9052202957155598336_n.jpg?_nc_cat=101&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=MwI_Au5sC60AX93Dkix&tn=8omYOUODC-SvWcRg&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=607e4783ada8c14a5d0fe50eaed35b74&oe=60E41393',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96557798_3065146790236420_838564679184809984_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=ydkcrs8kPykAX_0Fdn4&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=7884c93d73b2a9f806baf829c8f941b0&oe=60E4D7FB',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/fr/cp0/e15/q65/96688092_3065146896903076_7861539131082407936_n.jpg?_nc_cat=108&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=vqgGsFXTmO4AX82bX5z&_nc_ht=scontent.fhlz2-1.fna&tp=14&oh=379eb1c4551d74a13a4cafb07524288e&oe=60E6753F',
            ],
            'images_description': [
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
            ],
            'images_lowquality': [
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-9/cp0/e15/q65/p720x720/96724875_3065146506903115_4237164853036318720_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=SvpNqSK7ILIAX93ehWM&_nc_ht=scontent.fhlz2-1.fna&tp=3&oh=426e258c934177d9ded48435efaecc6c&oe=60E74054',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96657922_3065146630236436_9052202957155598336_n.jpg?_nc_cat=101&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=MwI_Au5sC60AX93Dkix&tn=8omYOUODC-SvWcRg&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=85385c57a98cbd698d746ddafc29a61c&oe=60E75F2D',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96557798_3065146790236420_838564679184809984_n.jpg?_nc_cat=103&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=ydkcrs8kPykAX_0Fdn4&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=ca962fe95d846cbd6e4e78b0884572c9&oe=60E51308',
                'https://scontent.fhlz2-1.fna.fbcdn.net/v/t1.6435-0/cp0/e15/q65/s640x640/96688092_3065146896903076_7861539131082407936_n.jpg?_nc_cat=108&ccb=1-3&_nc_sid=8024bb&efg=eyJpIjoidCJ9&_nc_ohc=vqgGsFXTmO4AX82bX5z&_nc_ht=scontent.fhlz2-1.fna&tp=9&oh=7e9da116d24a9faee2fe15c16d7dea8f&oe=60E3DD81',
            ],
            'images_lowquality_description': [
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
                'No photo description available.',
            ],
            'is_live': False,
            'likes': 1615,
            'link': 'https://www.nintendo.com/wallpapers/?fbclid=IwAR3uYocTphYdr6YYAznKWWdMBZ-p_Id3uNTFJ3_3lHwjnL3H7rRIEvb8yY8',
            'original_request_url': 3065154550235644,
            'post_id': '3065154550235644',
            'post_text': 'Check out these themed wallpapers and many more at the link '
            'below for your personal use! We hope you enjoy them!\n'
            '\n'
            'https://www.nintendo.com/wallpapers/',
            'post_url': 'https://facebook.com/story.php?story_fbid=3065154550235644&id=119240841493711',
            'reaction_count': 2117,
            'reactions': {
                'angry': 3,
                'care': 92,
                'haha': 4,
                'like': 1615,
                'love': 381,
                'wow': 22,
            },
            'reactors': [],
            'shared_post_id': None,
            'shared_post_url': None,
            'shared_text': '',
            'shared_time': None,
            'shared_user_id': None,
            'shared_username': None,
            'shares': 281,
            'text': 'Check out these themed wallpapers and many more at the link below '
            'for your personal use! We hope you enjoy them!\n'
            '\n'
            'https://www.nintendo.com/wallpapers/',
            'time': datetime.datetime(2020, 5, 13, 13, 1),
            'user_id': '119240841493711',
            'user_url': 'https://facebook.com/Nintendo/?refid=52&__tn__=C-R',
            'username': 'Nintendo',
            'video': None,
            'video_duration_seconds': None,
            'video_height': None,
            'video_id': None,
            'video_quality': None,
            'video_size_MB': None,
            'video_thumbnail': None,
            'video_watches': None,
            'video_width': None,
            'w3_fb_url': 'https://www.facebook.com/story.php?story_fbid=3065154550235644&id=119240841493711',
        }

        post = next(
            get_posts(post_urls=[3065154550235644], extra_info=True, cookies="cookies.txt")
        )

        fields_to_ignore = ["fetched_time", "link"]
        for field in fields_to_ignore:
            post.pop(field)  # Do not check this field
            expected_post.pop(field)

        assert post == expected_post

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

    def test_get_posts_with_extra_info_fields_presence(self):
        posts = list(
            get_posts(account='Nintendo', pages=2, cookies="cookies.txt", extra_info=True)
        )

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

    def test_smoketest(self):
        list(get_posts(account='Nintendo', pages=2))


@pytest.mark.vcr()
class TestGetGroupPosts:
    def test_get_group_posts(self):
        expected_post = {
            'available': True,
            'comments': 1,
            'comments_full': None,
            'factcheck': None,
            'image': None,
            'image_id': None,
            'image_ids': [],
            'image_lowquality': None,
            'images': [],
            'images_description': [],
            'images_lowquality': [],
            'images_lowquality_description': [],
            'is_live': False,
            'likes': 32,
            'link': None,
            'post_id': '1629606003787605',
            'post_text': 'Hola!, This group is aimed to create opportunities for South '
            'American students in Computer Science and related fields.\n'
            '\n'
            'Hope this will help us to know what we are doing in our work, '
            'achievements to be recognized, increase fairness in our area, '
            'and maybe conferences where we might meet.\n'
            '\n'
            'Professors and professionals are also welcomed to share their '
            'experiences and to collaborate among us and learn together.\n'
            '\n'
            'Some short rules for a happy co-existence:\n'
            '1. No business advertisement or spam.\n'
            '2. Topics relevant to Computing, Computer Science, Software '
            'Engineering, and Education.\n'
            '3. Political and religious advertisement are not allowed.',
            'post_url': 'https://m.facebook.com/groups/southamericansincomputing/permalink/1629606003787605/',
            'reaction_count': None,
            'reactions': None,
            'reactors': None,
            'shared_post_id': None,
            'shared_post_url': None,
            'shared_text': '',
            'shared_time': None,
            'shared_user_id': None,
            'shared_username': None,
            'shares': 0,
            'text': 'Hola!, This group is aimed to create opportunities for South '
            'American students in Computer Science and related fields.\n'
            '\n'
            'Hope this will help us to know what we are doing in our work, '
            'achievements to be recognized, increase fairness in our area, and '
            'maybe conferences where we might meet.\n'
            '\n'
            'Professors and professionals are also welcomed to share their '
            'experiences and to collaborate among us and learn together.\n'
            '\n'
            'Some short rules for a happy co-existence:\n'
            '1. No business advertisement or spam.\n'
            '2. Topics relevant to Computing, Computer Science, Software '
            'Engineering, and Education.\n'
            '3. Political and religious advertisement are not allowed.',
            'time': datetime.datetime(2018, 4, 4, 8, 2, 42),
            'user_id': 757122227,
            'user_url': 'https://facebook.com/omarflorez?groupid=117507531664134&refid=18&_ft_=top_level_post_id.1629606003787605%3Acontent_owner_id_new.757122227%3Apage_id.117507531664134%3Astory_location.6%3Atds_flgs.3%3Aott.AX_xo0_Tl6A-u34K%3Apage_insights.%7B%22117507531664134%22%3A%7B%22page_id%22%3A117507531664134%2C%22page_id_type%22%3A%22group%22%2C%22actor_id%22%3A757122227%2C%22dm%22%3A%7B%22isShare%22%3A0%2C%22originalPostOwnerID%22%3A0%7D%2C%22psn%22%3A%22EntGroupDescriptionChangeCreationStory%22%2C%22post_context%22%3A%7B%22object_fbtype%22%3A657%2C%22publish_time%22%3A1522785762%2C%22story_name%22%3A%22EntGroupDescriptionChangeCreationStory%22%2C%22story_fbid%22%3A%5B1629606003787605%5D%7D%2C%22role%22%3A1%2C%22sl%22%3A6%7D%7D&__tn__=C-R',
            'username': 'Omar U. Florez',
            'video': None,
            'video_duration_seconds': None,
            'video_height': None,
            'video_id': None,
            'video_quality': None,
            'video_size_MB': None,
            'video_thumbnail': None,
            'video_watches': None,
            'video_width': None,
            'w3_fb_url': None,
        }

        unset_cookies()
        post = next(get_posts(group=117507531664134))

        assert post == expected_post

    # todo: add a case with requesting a group post with start_url=None

    def test_smoketest(self):
        list(get_posts(group=117507531664134, pages=2))


@pytest.mark.vcr()
class TestGetPhotos:
    def test_smoketest(self):
        list(get_photos(account='Nintendo', pages=2))
