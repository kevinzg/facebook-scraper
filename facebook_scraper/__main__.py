from . import write_posts_to_csv
import argparse


def run():
    """facebook-scraper entry point when used as a script"""
    parser = argparse.ArgumentParser(
        prog='facebook-scraper', description="Scrape Facebook public pages without an API key",
    )
    parser.add_argument('account', type=str, help="Facebook account")
    parser.add_argument('-f', '--filename', type=str, help="Output filename")
    parser.add_argument('-p', '--pages', type=int, help="Number of pages to download", default=10)
    parser.add_argument('-g', '--group', action='store_true', help="Use group scraper")

    args = parser.parse_args()

    account_type = 'group' if args.group else 'account'
    kwargs = {account_type: args.account}

    write_posts_to_csv(**kwargs, filename=args.filename, pages=args.pages)


if __name__ == '__main__':
    run()
