from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum,hash
from tpch_modules import read_tpch_tables,build_enriched_df


def main():
    spark = (
        SparkSession.builder
        .appName("TPCH-Skew")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "12")


    base_path = "data/raw/tpch_sf1"
    df_dict = read_tpch_tables(spark,base_path)
    
    # Create master data.
    enriched_df = build_enriched_df(df_dict)

    # Introducing delibrate Skew.
    # Split ASIA vs non-ASIA
    asia_df = enriched_df.filter(col("r_name") == "ASIA")
    non_asia_df = enriched_df.filter(col("r_name") != "ASIA")

    # Create skew by duplicating ASIA rows (3x)
    skewed_asia = asia_df.union(asia_df).union(asia_df)

    # Combine back
    skewed_df = non_asia_df.union(skewed_asia)

    revenue_df = (
        skewed_df
        .withColumn(
            "revenue",
            col("l_extendedprice") * (1 - col("l_discount"))
        )
        .groupBy("r_name", "order_year")
        .agg(_sum("revenue").alias("total_revenue"))
    )
    revenue_df.show()


    spark.stop()


if __name__ == "__main__":
    main()