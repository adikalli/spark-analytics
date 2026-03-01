from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast,col, year, sum as _sum

def main():

    spark = (
        SparkSession.builder
        .appName("TPCHRevenueByRegionYear")
        .master("local[*]")
        .getOrCreate()
    )

    # spark.conf.set("spark.sql.shuffle.partitions", "8")


    # Base path inside container
    scale_factor = 1
    base_path = f"data/raw/tpch_sf{scale_factor}"

    # Read Parquet tables
    orders = spark.read.parquet(f"{base_path}/orders")
    lineitem = spark.read.parquet(f"{base_path}/lineitem")
    customer = spark.read.parquet(f"{base_path}/customer")
    nation = spark.read.parquet(f"{base_path}/nation")
    region = spark.read.parquet(f"{base_path}/region")

    # join fct and dim tables.
    joined_df = (
        lineitem
        .join(orders, lineitem.l_orderkey == orders.o_orderkey)
        .join(customer, orders.o_custkey == customer.c_custkey)
        .join(broadcast(nation), customer.c_nationkey == nation.n_nationkey)
        .join(broadcast(region), nation.n_regionkey == region.r_regionkey)
    )

    # Derive order year
    enriched_df = joined_df.withColumn(
        "order_year",
        year(col("o_orderdate"))
    )

    # Compute revenue KPI
    revenue_df = (
        enriched_df
        .withColumn(
            "revenue",
            col("l_extendedprice") * (1 - col("l_discount"))
        )
        .groupBy("r_name", "order_year")
        .agg(_sum("revenue").alias("total_revenue"))
    )
    revenue_df.show()

    # Write output
    output_path = f"data/output/tpch_sf{scale_factor}/revenue_by_region_year"

    (
        revenue_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()