import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from validation_functions import ValidationFunctions

validFunc = ValidationFunctions()

if __name__ == '__main__':
    
    # Setup the Configuration
    spark = SparkSession.builder.appName("HeyAuto Challenge").getOrCreate()
    
    # Load data in Spark DataFrame
    heyauto_csv = "data_heyAuto.csv"
    heyauto_df = spark.read.format("csv")\
                                    .option("inferSchema", "true")\
                                    .option("header", "true")\
                                    .load(heyauto_csv)
                                    
    # Transform Condition to convert U/N to Used/New
    heyauto_transformed_df = heyauto_df.withColumn("Condition", F.when(heyauto_df.Condition == "U", "Used")\
                                                .when(heyauto_df.Condition == "N", "New")\
                                                .otherwise(None))
    
    # Start Validation process by adding validation column 
    heyauto_val_df = validFunc.create_validation_column(heyauto_transformed_df, "failed")
    
    # Validation 1 - hasMake
    heyauto_val1_df = validFunc.has_feature(heyauto_val_df, "failed", "make", "hasNotMake")
    
    # Validation 2 - hasModel
    heyauto_val2_df = validFunc.has_feature(heyauto_val1_df, "failed", "model", "hasNotModel")
    
    # Validation 3 - hasYear
    heyauto_val3_df = validFunc.has_feature(heyauto_val2_df, "failed", "year", "hasNotYear")
    
    # Validation 4 - hasCondition
    heyauto_val4_df = validFunc.has_feature(heyauto_val3_df, "failed", "Condition", "hasNotCondition")
    
    # Validation 5 - validVin
    heyauto_val5_df = validFunc.is_valid_length(heyauto_val4_df, "failed", "vin", 17, "notValidVin")
    
    # Validation 6 - validYear
    year_query = "year BETWEEN 1990 AND 2024 OR year IS NULL"
    heyauto_val6_df = validFunc.is_query_valid(heyauto_val5_df, "failed", year_query, "notValidYear")
    
    # Get only accepted vehicles
    heyauto_accepted_df = heyauto_val6_df.select(F.col("vin"))\
                                    .filter(F.size("failed") == 0)
    
    # Get only rejected vehicles 
    heyauto_rejected_df = heyauto_val6_df.select(F.col("vin"), F.col("failed"))\
                                    .filter(F.size("failed") != 0)
                                    
    # Display results 
    print("Accepted Vehicles :")
    print(heyauto_accepted_df.show())
    
    print("Rejected Vehicles :")
    print(heyauto_rejected_df.show(truncate=False))
                                    

    
    
    
    
