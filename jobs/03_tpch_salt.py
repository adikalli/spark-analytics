from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from tpch_modules import read_tpch_tables,build_enriched_df,introduce_skew,compute_revenue


def main():
    spark = (
        SparkSession.builder
        .appName("TPCH-Salting")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    NUM_SALTS = 6
    base_path = "data/raw/tpch_sf1"

    df_dict = read_tpch_tables(spark,base_path)
    enriched_df = build_enriched_df(df_dict)
    enriched_df = enriched_df.filter(f.col('order_year')==1996)

    enriched_rev_df = compute_revenue(enriched_df)
    skewed_df = introduce_skew(enriched_rev_df)

    print("=== BEFORE SALTING: Partition Distribution ===")

    before_dist = (
        skewed_df
        .withColumn("partition_id", f.spark_partition_id())
        .groupBy("partition_id")
        .agg(f.count("*").alias("row_count"))
        .orderBy("partition_id")
    )

    before_dist.show(50, False)

    # Add Salt Column
    salted_df = skewed_df.withColumn("salt", f.floor(f.rand() * NUM_SALTS).cast("int"))

    # First Aggregation (Salted)
    partial_agg_df = (
        salted_df.groupBy("r_name", "order_year","salt")
            .agg(f.sum("revenue").alias("partial_revenue"))
    )
    
    print("=== AFTER SALTING: Partition Distribution ===")

    after_dist = (
        partial_agg_df
        .withColumn("partition_id", f.spark_partition_id())
        .groupBy("partition_id")
        .agg(f.count("*").alias("row_count"))
        .orderBy("partition_id")
    )

    after_dist.show(50, False)
    # Final Aggregation (Remove Salt)
    final_df = (
        partial_agg_df
        .groupBy("r_name", "order_year")
        .agg(f.sum("partial_revenue").alias("total_revenue"))
    )
    final_df.show()

    spark.stop()


if __name__ == "__main__":
    main()