import argparse
import logging
import pathlib
import datetime
import sys
import locale
import json
import csv

from . import enable_logging, write_posts_to_csv, get_profile


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
        '-d',
        '--days-limit',
        dest='days_limit',
        default=3650,
        type=int,
        help="Number of days to download",
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
        '-m',
        '--matching',
        type=str,
        default=".+",
        help='Filter to just posts matching regex expression',
    )
    parser.add_argument(
        '-nm',
        '--not-matching',
        type=str,
        help='Filter to just posts not matching regex expression',
    )
    parser.add_argument(
        '--extra-info ',
        dest='extra_info',
        action='store_true',
        help="Try to do an extra request to get the post reactions. Default is False",
        default=False,
    )
    parser.add_argument(
        '--use-youtube-dl',
        dest='youtube_dl',
        action='store_true',
        help='Use Youtube-DL for (high-quality) video extraction. You need to have youtube-dl installed on your environment. Default is False.',
        default=False,
    )
    parser.add_argument(
        '--profile',
        action='store_true',
        help="Extract an account's profile",
        default=False,
    )
    parser.add_argument(
        '--friends', type=int, help='When extracting a profile, how many friends to extract'
    )
    parser.add_argument(
        '-ppp',
        '--posts-per-page',
        dest='posts_per_page',
        default=4,
        type=int,
        help="Number of posts to fetch per page",
    )
    parser.add_argument(
        '--source',
        action='store_true',
        help="Include HTML source",
        default=False,
    )

    args = parser.parse_args()

    # Enable logging
    if args.verbose > 0:
        args.verbose = min(args.verbose, 3)
        level = {1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}[args.verbose]
        enable_logging(level)

    if args.profile:
        # Set a default filename, based on the account name with the appropriate extension
        if args.filename is None:
            args.filename = str(args.account) + "_profile." + args.format

        if args.encoding is None:
            encoding = locale.getpreferredencoding()

        if args.filename == "-":
            output_file = sys.stdout
        else:
            output_file = open(args.filename, 'w', newline='', encoding=encoding)

        profile = get_profile(args.account, friends=args.friends, cookies=args.cookies)

        if args.format == "json":
            json.dump(profile, output_file, default=str, indent=4)
        else:
            dict_writer = csv.DictWriter(output_file, profile.keys())
            dict_writer.writeheader()
            dict_writer.writerow(profile)
        output_file.close()
    else:
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
            "not_matching": args.not_matching,
            "options": {
                "reactions": args.reactions,
                "reactors": args.reactors,
                "comments": args.comments,
                "allow_extra_requests": args.allow_extra_requests,
                "posts_per_page": args.posts_per_page,
            },
            "youtube_dl": args.youtube_dl,
            "extra_info": args.extra_info,
            "remove_source": not args.source,
        }

        write_posts_to_csv(
            **kwargs,
            filename=args.filename,
            pages=args.pages,
            encoding=args.encoding,
            dump_location=args.dump_location,
        )


if __name__ == '__main__':
    run()
