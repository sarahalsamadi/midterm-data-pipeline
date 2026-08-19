import json
import os
from datetime import datetime, timezone


RESULTS_FILE = "reports/results.json"


def save_run_metrics(metrics):
    os.makedirs("reports", exist_ok=True)

    metrics["recorded_at"] = datetime.now(timezone.utc).isoformat()

    if os.path.exists(RESULTS_FILE):
        try:
            with open(
                RESULTS_FILE,
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    if not isinstance(data, list):
        data = []

    data.append(metrics)

    with open(
        RESULTS_FILE,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Metrics saved to: {RESULTS_FILE}")