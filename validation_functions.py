import pyspark.sql.functions as F

class ValidationFunctions:
    
    def __init__(self):
        pass
    
    def create_validation_column(self, input_df, column_name):
        output_df = input_df.withColumn(column_name, F.array())
        return output_df
    
    def has_feature(self, input_df, valid_column, feature, failure_statement):
        output_df = input_df.withColumn(valid_column, 
                                                F.when(F.expr("{} is NULL".format(feature)), F.array_union(F.col(valid_column), F.array(F.lit(failure_statement))))\
                                                .otherwise(F.col(valid_column)))
        return output_df
    
    def is_valid_length(self, input_df, valid_column, feature, length, failure_statement):
        output_df = input_df.withColumn(valid_column, 
                                                F.when(F.length(feature) != length, F.array_union(F.col(valid_column), F.array(F.lit(failure_statement))))\
                                                .otherwise(F.col(valid_column)))
        return output_df
    
    
    def is_query_valid(self, input_df, valid_column, query, failure_statement):
        output_df = input_df.withColumn(valid_column, 
                                                F.when(F.expr(query), F.array_union(F.col(valid_column), F.array(F.lit(failure_statement))))\
                                                .otherwise(F.col(valid_column)))
        return output_df