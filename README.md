# data-quality-monitor

great expectations is overkill for most of what i need. this does 80% of the job in ~500 lines.

define data quality checks in YAML, run them against any SQL database, get pass/fail results with details. optional slack alerts.

## install

```
pip install -e .
```

## usage

```bash
# run checks
python -m dqm run checks/example_checks.yaml --connection sqlite:///data.db

# validate config without running
python -m dqm validate checks/example_checks.yaml
```

## check types

- **null_rate** — fail if null percentage exceeds threshold
- **unique** — fail if column has duplicate values
- **value_range** — fail if values fall outside min/max
- **row_count** — fail if table has fewer/more rows than expected
- **freshness** — fail if table hasn't been updated recently

## config

```yaml
tables:
  - name: orders
    checks:
      - type: null_rate
        column: order_id
        max_null_pct: 0
      - type: row_count
        min: 1000
      - type: freshness
        column: updated_at
        max_hours: 24
```

see `checks/example_checks.yaml` for the full format.

## alerting

add a slack webhook to get notified on failures:
```yaml
alerting:
  slack_webhook: https://hooks.slack.com/services/xxx
```
