
from pathlib import Path

REQUIRED_OUTPUTS = [
    "bronze/events.parquet",
    "silver/events.parquet",
    "gold/orders.parquet",
    "gold/daily_metrics.parquet",
    "report.json",
]


def test_required_output_names_are_documented():
    readme = Path("README.md").read_text()
    for name in REQUIRED_OUTPUTS:
        assert name in readme
