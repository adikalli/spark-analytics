from pyspark.sql import SparkSession

from tpch_modules import tpch_runner


def main():
    spark = (
        SparkSession.builder
        .appName("TPCH-Base")
        .getOrCreate()
    )

    base_path = "data/raw/tpch_sf1"

    tpch_revenue = tpch_runner(spark,base_path)
    tpch_revenue.show()

    # Check Distribution of Data.
    tpch_revenue.groupBy('r_name','order_year')\
                .agg(count('*').alias('cnt'))\
                .orderBy(desc('r_name'),desc('cnt'))\
                .show(40)
    # Data is fairly distributed.
    # tpch_revenue.write.mode("overwrite").parquet(
    #     "data/output/tpch_sf1/tpch_base_revenue"
    # )

    spark.stop()


if __name__ == "__main__":
    main()