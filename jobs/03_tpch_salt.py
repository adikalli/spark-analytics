from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum,hash
from tpch_modules import read_tpch_tables,build_enriched_df,introduce_skew,compute_revenue


def main():
    spark = (
        SparkSession.builder
        .appName("TPCH-Salting")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "12")

    NUM_SALTS = 8
    base_path = "data/raw/tpch_sf1"

    df_dict = read_tpch_tables(spark,base_path)
    enriched_df = build_enriched_df(df_dict)
    enriched_rev_df = compute_revenue(enriched_df)
    skewed_df = introduce_skew(enriched_rev_df)

    # Add Salt Column
    salted_df = skewed_df.withColumn(
        "salt",
        (hash(col("o_custkey")) % NUM_SALTS)
    )

    # First Aggregation (Salted)
    partial_agg_df = (
        salted_df.groupBy("r_name", "order_year", "salt")
            .agg(_sum("revenue").alias("partial_revenue"))
    )
    # Final Aggregation (Remove Salt)
    final_df = (
        partial_agg_df
        .groupBy("r_name", "order_year")
        .agg(_sum("partial_revenue").alias("total_revenue"))
    )
    final_df.show()

    spark.stop()


if __name__ == "__main__":
    main()