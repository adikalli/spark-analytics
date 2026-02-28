"""
Utility to generate synthetic sales data.

Columns:
    - order_id (int)
    - customer_id (int)
    - amount (float)
    - country (str)

Default rows: 1000
"""

import csv
import random
from pathlib import Path
from typing import List, Optional


COUNTRIES: List[str] = ["IN", "US", "DE", "UK", "FR", "SG", "AU"]


def generate_sales_data(
    num_rows: int = 1000,
    output_path: str = "data/input/sales_data.csv",
    seed: Optional[int] = None,
) -> None:
    """
    Generate synthetic sales data and write to CSV.

    Args:
        num_rows (int): Number of rows to generate. Default is 1000.
        output_path (str): File path to write the CSV.
        seed (Optional[int]): Random seed for reproducibility.
    """

    if seed is not None:
        random.seed(seed)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open(mode="w", newline="") as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow(["order_id", "customer_id", "amount", "country"])

        for order_id in range(1, num_rows + 1):
            customer_id = random.randint(1, 500)
            amount = round(random.uniform(50, 1000), 2)
            country = random.choice(COUNTRIES)

            writer.writerow([order_id, customer_id, amount, country])


if __name__ == "__main__":
    generate_sales_data()