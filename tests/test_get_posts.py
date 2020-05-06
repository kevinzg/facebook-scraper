from facebook_scraper import get_posts


def test_get_posts():
    posts = list(get_posts(account='Nintendo'))

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
