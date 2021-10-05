import argparse
import logging
import pathlib
import datetime

from . import enable_logging, write_posts_to_csv


def run():
    """facebook-scraper entry point when used as a script"""
    parser = argparse.ArgumentParser(
        prog='facebook-scraper',
        description="Scrape Facebook public pages without an API key",
    )
    parser.add_argument('account', type=str, help="Facebook account or group")
    parser.add_argument('-f', '--filename', type=str, help="Output filename")
    parser.add_argument('-p', '--pages', type=int, help="Number of pages to download", default=10)
    parser.add_argument(
        '-s', '--sleep', type=float, help="How long to sleep for between posts", default=0
    )
    parser.add_argument(
        '-t',
        '--timeout',
        type=int,
        help="How long to wait in seconds for Facebook servers before aborting",
        default=10,
    )
    parser.add_argument('-g', '--group', action='store_true', help="Use group scraper")
    parser.add_argument('-v', '--verbose', action='count', help="Enable logging", default=0)
    parser.add_argument('-c', '--cookies', type=str, help="Path to a cookies file")
    parser.add_argument('--comments', action='store_true', help="Extract comments")
    parser.add_argument('-r', '--reactions', action='store_true', help="Extract reactions")
    parser.add_argument('-rs', '--reactors', action='store_true', help="Extract reactors")
    parser.add_argument(
        '--dump',
        type=pathlib.Path,
        dest='dump_location',
        help="Location where to save the HTML source of the posts (useful for debugging)",
        default=None,
    )
    parser.add_argument(
        '--encoding',
        action='store',
        help="Encoding for the output file",
        default=None,
    )
    parser.add_argument(
        '-fmt',
        '--format',
        type=str.lower,
        choices=["csv", "json"],
        default="csv",
        help="What format to export as",
    )
    parser.add_argument(
        '-d', '--days-limit', dest='days_limit', default=3650, type=int, help="Number of days to download"
    )
    parser.add_argument(
        '-rf',
        '--resume-file',
        type=str,
        help="Filename to store the last pagination URL in, for resuming",
    )
    parser.add_argument(
        '-ner',
        '--no-extra-requests',
        dest='allow_extra_requests',
        action='store_false',
        help="Disable making extra requests (for things like high quality image URLs)",
    )
    parser.add_argument(
        '-k',
        '--keys',
        type=lambda s: s.split(sep=","),
        help="Comma separated list of which keys or columns to return. This lets you filter to just your desired outputs.",
    )
    parser.add_argument(
        '-m', '--matching', type=str, default=".+", help='Filter to just posts matching regex expression'
    )

    args = parser.parse_args()

    # Choose the right argument to pass to write_posts_to_csv (group or account)
    account_type = 'group' if args.group else 'account'
    kwargs = {
        account_type: args.account,
        "format": args.format,
        "days_limit": args.days_limit,
        "resume_file": args.resume_file,
        "cookies": args.cookies,
        "timeout": args.timeout,
        "sleep": args.sleep,
        "keys": args.keys,
        "matching": args.matching,
        "options": {
            "reactions": args.reactions,
            "reactors": args.reactors,
            "comments": args.comments,
            "allow_extra_requests": args.allow_extra_requests,
        },
    }

    # Enable logging
    if args.verbose > 0:
        args.verbose = min(args.verbose, 3)
        level = {1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}[args.verbose]
        enable_logging(level)

    write_posts_to_csv(
        **kwargs,
        filename=args.filename,
        pages=args.pages,
        encoding=args.encoding,
        dump_location=args.dump_location,
    )


if __name__ == '__main__':
    run()
