import argparse
from data_generator import generate_sales_data

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate (default: 1000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/input/sales_data.csv",
        help="Output file path",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )

    args = parser.parse_args()

    generate_sales_data(
        num_rows=args.rows,
        output_path=args.output,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()