from . import write_posts_to_csv


def _main():
    """facebook-scraper entry point when used as a script"""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('account', type=str, help="Facebook account")
    parser.add_argument('-f', '--filename', type=str, help="Output filename")
    parser.add_argument('-p', '--pages', type=int, help="Number of pages to download", default=10)

    args = parser.parse_args()

    write_posts_to_csv(account=args.account, filename=args.filename, pages=args.pages)


if __name__ == '__main__':
    _main()
