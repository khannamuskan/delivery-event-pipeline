
from __future__ import annotations

import os
from pathlib import Path

from generator.generate import generate_dataset
from candidate.pipeline import run


def main() -> None:
    seed = int(os.getenv("SEED", "42"))
    orders = int(os.getenv("ORDERS", "1000"))

    # The image mounts /app; outside the container fall back to the repository
    # directory so `python main.py` is runnable locally without edits.
    base_dir = Path("/app") if Path("/app").is_dir() else Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    output_dir = base_dir / "output"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = data_dir / "events.jsonl"

    stats = generate_dataset(
        seed=seed,
        orders=orders,
        output_path=input_path,
        include_truth=False,
    )

    print("EVENT GENERATOR")
    print("===============")
    print(f"Seed: {seed}")
    print(f"Orders requested: {orders}")
    print(f"Raw events generated: {stats['raw_events']}")
    print()

    run(input_path=input_path, output_dir=output_dir)


if __name__ == "__main__":
    main()
