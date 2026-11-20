"""Command-line entry points for cohort replay workers and node coordination."""
import argparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('worker', 'coordinator'):
        command = commands.add_parser(name)
        command.add_argument('--config', required=True)
        command.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if args.command == 'worker':
        from .runner import run
    else:
        from .coordinator import run
    run(args.config, args.once)


if __name__ == '__main__':
    main()
