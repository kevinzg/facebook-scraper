from facebook_scraper.utils import parse_datetime


class TestParseDate:
    dates = [
        'Oct 1 at 1:00 PM',
        'Oct 1 at 11:00 PM',
        'Oct 16 at 1:00 PM',
        'Oct 16 at 11:00 PM',
        'October 1 at 1:00 PM',
        'October 1 at 11:00 PM',
        'October 16 at 1:00 PM',
        'October 16 at 11:00 PM',
        'October 1, 2019 at 1:00 PM',
        'October 1, 2019 at 11:00 PM',
        'October 16, 2019 at 1:00 PM',
        'October 16, 2019 at 11:00 PM',
        'Yesterday at 1:00 PM',
        'Yesterday at 11:00 PM',
        'Today at 1:00 PM',
        'Today at 11:00 PM',
        'Yesterday at 1:00 PM',
        'Yesterday at 11:00 PM',
        'Yesterday at 15:28',
        '7 November at 20:01',
        '1h',
        '16h',
        '1hrs',
        '16hrs',
        '1 hr',
        '16 hrs',
        '1 min',
        '50 mins',
    ]

    def test_all_dates(self):
        for date in self.dates:
            try:
                assert parse_datetime(date) is not None
            except AssertionError as e:
                print(f'Failed to parse {date}')
                raise e
