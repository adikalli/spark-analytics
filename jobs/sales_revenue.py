from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, expr

def calculate_sales_revenue(spark: SparkSession,input_path: str,show_output: bool = True) -> DataFrame:
    """
    Calculate the total sales revenue from the sales data.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df_revenue = df.groupBy("country").agg(expr("sum(amount) as total_revenue"))

    if show_output:
        print("--------------------------------")
        print("Input Data Row Count:")
        print(df.count())
        print("--------------------------------")
        print("Sales Revenue by Country:")
        df_revenue.show()
        print("--------------------------------")
    return df_revenue

def main():
    spark = SparkSession.builder.appName("Sales Revenue").getOrCreate()
    input_path = "data/input/sales_data.csv"
    df = calculate_sales_revenue(spark, input_path, show_output=True)
    df.write.parquet("data/output/sales_revenue",mode="overwrite")
    print("Sales Revenue data saved to data/output/sales_revenue")


if __name__ == "__main__":
    main()