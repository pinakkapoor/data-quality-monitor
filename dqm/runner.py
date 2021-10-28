"""
run checks from yaml config
"""

import yaml
from sqlalchemy import create_engine
from .checks import CHECK_REGISTRY, CheckResult


def load_checks(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run_checks(config_path, connection_string):
    """run all checks defined in config, return results"""
    config = load_checks(config_path)
    engine = create_engine(connection_string)

    results = []

    with engine.connect() as conn:
        for table_config in config.get('tables', []):
            table_name = table_config['name']

            for check_config in table_config.get('checks', []):
                check_type = check_config['type']
                check_fn = CHECK_REGISTRY.get(check_type)

                if check_fn is None:
                    results.append(CheckResult(
                        check_type=check_type, table=table_name,
                        column='?', passed=False,
                        message=f'unknown check type: {check_type}',
                    ))
                    continue

                try:
                    result = check_fn(conn, table_name, check_config)
                    results.append(result)
                except Exception as e:
                    results.append(CheckResult(
                        check_type=check_type, table=table_name,
                        column=check_config.get('column', '?'),
                        passed=False, message=f'error: {e}',
                    ))

    return results


def print_results(results):
    """print check results to console"""
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    for r in results:
        status = '✓' if r.passed else '✗'
        print(f'  {status} [{r.check_type:12s}] {r.table:20s} {r.message}')

    print(f'\n{passed} passed, {failed} failed out of {len(results)} checks')
    return failed == 0
