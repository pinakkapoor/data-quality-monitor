"""
data quality check implementations
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from sqlalchemy import text


@dataclass
class CheckResult:
    check_type: str
    table: str
    column: str
    passed: bool
    message: str
    value: float = None
    threshold: float = None


def null_rate_check(conn, table, config):
    """check null rate for a column"""
    column = config['column']
    max_pct = config.get('max_null_pct', 0)

    result = conn.execute(text(
        f'SELECT COUNT(*) as total, '
        f'SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) as nulls '
        f'FROM {table}'
    ))
    row = result.fetchone()
    total, nulls = row[0], row[1]
    null_pct = (nulls / total * 100) if total > 0 else 0

    passed = null_pct <= max_pct
    msg = f'{column}: {null_pct:.2f}% nulls (threshold: {max_pct}%)'

    return CheckResult(
        check_type='null_rate', table=table, column=column,
        passed=passed, message=msg, value=null_pct, threshold=max_pct,
    )


def unique_check(conn, table, config):
    """check that a column has unique values"""
    column = config['column']

    result = conn.execute(text(
        f'SELECT COUNT(*) as total, COUNT(DISTINCT "{column}") as distinct_count '
        f'FROM {table}'
    ))
    row = result.fetchone()
    total, distinct = row[0], row[1]
    dupes = total - distinct

    passed = dupes == 0
    msg = f'{column}: {dupes} duplicates found ({total} total, {distinct} distinct)'

    return CheckResult(
        check_type='unique', table=table, column=column,
        passed=passed, message=msg, value=dupes,
    )


def value_range_check(conn, table, config):
    """check values fall within expected range"""
    column = config['column']
    min_val = config.get('min')
    max_val = config.get('max')

    result = conn.execute(text(
        f'SELECT MIN("{column}") as min_val, MAX("{column}") as max_val FROM {table}'
    ))
    row = result.fetchone()
    actual_min, actual_max = row[0], row[1]

    issues = []
    if min_val is not None and actual_min is not None and float(actual_min) < min_val:
        issues.append(f'min {actual_min} < {min_val}')
    if max_val is not None and actual_max is not None and float(actual_max) > max_val:
        issues.append(f'max {actual_max} > {max_val}')

    passed = len(issues) == 0
    msg = f'{column}: range [{actual_min}, {actual_max}]'
    if issues:
        msg += f' — violations: {", ".join(issues)}'

    return CheckResult(
        check_type='value_range', table=table, column=column,
        passed=passed, message=msg,
    )


def row_count_check(conn, table, config):
    """check table has expected number of rows"""
    min_rows = config.get('min', 0)
    max_rows = config.get('max', float('inf'))

    result = conn.execute(text(f'SELECT COUNT(*) as cnt FROM {table}'))
    count = result.fetchone()[0]

    passed = min_rows <= count <= max_rows
    msg = f'{table}: {count:,} rows (expected: {min_rows:,} - {max_rows:,})'

    return CheckResult(
        check_type='row_count', table=table, column='*',
        passed=passed, message=msg, value=count,
    )


def freshness_check(conn, table, config):
    """check that table was updated recently"""
    column = config['column']
    max_hours = config.get('max_hours', 24)

    result = conn.execute(text(f'SELECT MAX("{column}") as latest FROM {table}'))
    row = result.fetchone()
    latest = row[0]

    if latest is None:
        return CheckResult(
            check_type='freshness', table=table, column=column,
            passed=False, message=f'{column}: no data found',
        )

    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest)

    age_hours = (datetime.now() - latest).total_seconds() / 3600
    passed = age_hours <= max_hours
    msg = f'{column}: last update {age_hours:.1f}h ago (threshold: {max_hours}h)'

    return CheckResult(
        check_type='freshness', table=table, column=column,
        passed=passed, message=msg, value=age_hours, threshold=max_hours,
    )


CHECK_REGISTRY = {
    'null_rate': null_rate_check,
    'unique': unique_check,
    'value_range': value_range_check,
    'row_count': row_count_check,
    'freshness': freshness_check,
}
