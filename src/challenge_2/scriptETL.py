from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


spark = SparkSession \
    .builder \
    .appName("Bitso Challenge 2") \
    .master("local") \
    .getOrCreate()
    
    # inputs
def load_dataframe(filepath: str, format: str)-> DataFrame:
    # for case simplification, I'll inferSchema
    return spark \
        .read \
        .format(format) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(filepath)

def save_dataframe_parquet(df: DataFrame, filepath: str):
    df \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .parquet(filepath)

def with_interface(deposits: DataFrame)->DataFrame:
    return deposits \
        .withColumn("interface", F.lit("default"))

def with_direction(df: DataFrame, direction: str)->DataFrame:
    return df \
        .withColumn("direction", F.lit(direction))

def unify_transactions(deposits: DataFrame, withdrawals: DataFrame)-> DataFrame:
    return deposits \
        .unionByName(withdrawals)

def main():

    # for this case specifically, I used the CSVs, however
    # we can setup a JDBC connector for spark to pull data from postgres
    # or create a task in airflow to extract data from postgres and either
    # use Xcom (for small data), post messages to an event processor (e.g. Kafka)
    # or write to a data lake like S3 as avro, parquet.

    load_filepaths = {
        "deposits":"src/challenge_2/resources/deposit_sample_data.csv",
        "events":"src/challenge_2/resources/event_sample_data.csv",
        "users":"src/challenge_2/resources/user_id_sample_data.csv",
        "user_level":"src/challenge_2/resources/user_level_sample_data.csv",
        "withdrawals":"src/challenge_2/resources/withdrawals_sample_data.csv"
    }

    # write_filepaths = {
    #     transactions:"src/challenge_2/resources/landing/transactions.parquet",
    #     events:"src/challenge_2/resources/landing/events.parquet",
    #     users:"src/challenge_2/resources/landing/users.parquet",
    #     user_level:"src/challenge_2/resources/landing/user_level.parquet"
    # }

    deposits = load_dataframe(load_filepaths.get("deposits"), "csv")
    events = load_dataframe(load_filepaths.get("events"), "csv")
    users = load_dataframe(load_filepaths.get("users"), "csv")
    user_level = load_dataframe(load_filepaths.get("user_level"), "csv")
    withdrawals = load_dataframe(load_filepaths.get("withdrawals"), "csv")

    deposits_enhanced = deposits \
        .transform(with_interface) \
        .transform(with_direction, "in")
    
    withdrawals_enhanced = withdrawals \
        .transform(with_direction, "out")
    
    transactions = unify_transactions(deposits_enhanced, withdrawals_enhanced)

    write_filepaths = {
        events:"src/challenge_2/resources/landing/events.parquet",
        users:"src/challenge_2/resources/landing/users.parquet",
        user_level:"src/challenge_2/resources/landing/user_level.parquet",
        transactions:"src/challenge_2/resources/landing/transactions.parquet"
    }

    for table, filepath in write_filepaths.items():
        save_dataframe_parquet(table, filepath)
        
if __name__ == "__main__":
    main()