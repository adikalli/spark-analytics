import os
import duckdb
from pathlib import Path


def export_tpch_data(scale_factor: int = 1,output_dir: str = "data/raw/"):
    """
    Downloads TPCH data using scale factor specified using DuckDB extension
    Default scale factor is 1.
    Default output directory is data/raw/tpch_sf{scale_factor}.
    All tables are exported as Parquet files.
    Scale factor usually ranges between 0.1 and 10000 (31mb to 379gb -parquet files)

    Args:
        scale_factor (int): Scale factor for TPCH data generation. Default is 1.
        output_dir (str): Output directory for TPCH data. Default is data/raw/.

    Returns:
        None
    """
    # if scale_factor < 0.01 or scale_factor > 10000:
    #     raise ValueError("Scale factor should be between 0.1 and 10000")

    output_dir = f"{output_dir}/tpch_sf{scale_factor}"
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Exporting TPCH SF{scale_factor} data to: {output_path.resolve()}")

    # Connect to DuckDB (in-memory)
    con = duckdb.connect(database=":memory:")

    # Install & load tpch extension
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")

    # Generate TPCH data at specified scale factor. (e.g. SF=10)
    print(f"Generating TPCH data (SF={scale_factor})...")
    con.execute(f"CALL dbgen(sf={scale_factor});")

    # Get list of TPCH tables
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()

    print("Exporting tables to Parquet...")

    for (table_name,) in tables:
        parquet_file = output_path / f"{table_name}.parquet"

        print(f"Writing {table_name} -> {parquet_file}")

        con.execute(f"""
            COPY {table_name}
            TO '{parquet_file}'
            (FORMAT PARQUET);
        """)

    con.close()
    print(f"TPCH SF{scale_factor} export completed successfully.")


if __name__ == "__main__":
    export_tpch_data(scale_factor=10,output_dir="data/raw/")