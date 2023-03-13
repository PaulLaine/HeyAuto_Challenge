import findspark
findspark.init()
from pyspark.sql import SparkSession

class PostgresConnector():
    
    def __init__(self, jars_url):
        self.spark = SparkSession \
                .builder \
                .appName("HeyAuto_Session") \
                .config("spark.jars", jars_url) \
                .getOrCreate()
    
    def read_table_from_postgres(self, db_url, db_name, username, password):
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_name) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
            
    def write_table_to_postgres(self, output_df, db_url, db_name, username, password):
        output_df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_name) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .save()