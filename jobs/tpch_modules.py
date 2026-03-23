from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col, year, sum as _sum

def read_tpch_tables(spark, base_path):
    return {
        "orders": spark.read.parquet(f"{base_path}/orders"),
        "lineitem": spark.read.parquet(f"{base_path}/lineitem"),
        "customer": spark.read.parquet(f"{base_path}/customer"),
        "nation": spark.read.parquet(f"{base_path}/nation"),
        "region": spark.read.parquet(f"{base_path}/region"),
    }

def build_enriched_df(tables):
    return (
        tables["lineitem"]
        .join(tables["orders"], col("l_orderkey") == col("o_orderkey"))
        .join(tables["customer"], col("o_custkey") == col("c_custkey"))
        .join(tables["nation"], col("c_nationkey") == col("n_nationkey"))
        .join(tables["region"], col("n_regionkey") == col("r_regionkey"))
        .withColumn("order_year", year(col("o_orderdate")))
    )

def compute_revenue(df):
    return df.withColumn(
        "revenue",
        col("l_extendedprice") * (1 - col("l_discount"))
    )

def aggregate_revenue(df):
    return (
        df.groupBy("r_name", "order_year")
        .agg(_sum("revenue").alias("total_revenue"))
    )

def introduce_skew(df, region_name="ASIA", multiplier=3):
    
    skew_df = df.filter(col("r_name") == region_name)
    non_skew_df = df.filter(col("r_name") != region_name).limit(50)
                    # .sample(withReplacement=False, fraction=0.01, seed=42)

    # new region
    new_region = non_skew_df.limit(5)\
                .withColumn('r_name',lit('AUSTRALIA'))

    for _ in range(multiplier - 1):
        skew_df = skew_df.union(df.filter(col("r_name") == region_name))

    skew_df = skew_df.union(new_region)
    return non_skew_df.union(skew_df)

def tpch_runner(spark,data_path):
    """
    TPCH Runner
    """    
    print("Reading TPCH Data.")
    df_dict = read_tpch_tables(spark,data_path)
    for tbl,df in df_dict.items():
        print(f"Fetching info for table:{tbl}")
        count = df.count()
        df.printSchema()
        df.show(20,truncate=False)
    print("Creating Master Data.")
    enriched_df = build_enriched_df(df_dict)
    enriched_df.show(10,truncate=False)
    print("Deriving Revenue.")
    revenue_df = compute_revenue(enriched_df)
    print("Computing Revenue.")
    revenue_df = aggregate_revenue(revenue_df)
    revenue_df.show()

def main():
    """
    Main
    """
    scale_factor = 1
    base_path = f"data/raw/tpch_sf{scale_factor}"
    spark = SparkSession.builder.appName("TPCH Reader").getOrCreate()
    df_tpch_revenue = tpch_runner(spark,base_path)
    df_tpch_revenue.show()

if __name__ == "__main__":
    main()

