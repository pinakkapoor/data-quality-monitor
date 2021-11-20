"""
alerting — send check results to slack or console
"""

import json
import requests


def send_slack_alert(webhook_url, results, config_name=''):
    """send failed check results to slack"""
    failures = [r for r in results if not r.passed]
    if not failures:
        return

    blocks = [
        {
            'type': 'header',
            'text': {
                'type': 'plain_text',
                'text': f'🔴 Data Quality Alert — {len(failures)} check(s) failed',
            }
        },
    ]

    if config_name:
        blocks.append({
            'type': 'section',
            'text': {'type': 'mrkdwn', 'text': f'Config: `{config_name}`'}
        })

    for r in failures:
        blocks.append({
            'type': 'section',
            'text': {
                'type': 'mrkdwn',
                'text': f'*{r.check_type}* on `{r.table}.{r.column}`\n{r.message}',
            }
        })

    payload = {'blocks': blocks}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f'slack alert failed: {resp.status_code} {resp.text}')
        else:
            print(f'sent slack alert for {len(failures)} failures')
    except requests.RequestException as e:
        print(f'slack alert error: {e}')
