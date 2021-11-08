"""
CLI for data quality monitor
"""

import sys
import click
from .runner import run_checks, print_results, load_checks
from .alerting import send_slack_alert


@click.group()
def main():
    """dqm: data quality monitoring"""
    pass


@main.command()
@click.argument('config')
@click.option('--connection', '-c', required=True, help='database connection string')
@click.option('--slack-webhook', help='slack webhook URL for alerts')
def run(config, connection, slack_webhook):
    """run data quality checks"""
    results = run_checks(config, connection)
    all_passed = print_results(results)

    if slack_webhook:
        send_slack_alert(slack_webhook, results, config_name=config)

    sys.exit(0 if all_passed else 1)


@main.command()
@click.argument('config')
def validate(config):
    """validate a checks config file"""
    try:
        cfg = load_checks(config)
        tables = cfg.get('tables', [])
        total_checks = sum(len(t.get('checks', [])) for t in tables)
        print(f'config valid: {len(tables)} tables, {total_checks} checks')
    except Exception as e:
        print(f'config invalid: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
