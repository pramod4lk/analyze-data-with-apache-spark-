from pyspark.sql.functions import split, col

#  loads data from all .csv files in the orders folder into a dataframe named order_details and then displays the first five records.
order_details = spark.read.csv('/orders/*.csv', header=True, inferSchema=True)
display(order_details.limit(5))

"""
he code uses the split function to separate the values in the CustomerName column into two new columns named FirstName and LastName. 
Then it uses the drop method to delete the original CustomerName column.
"""

# Create the new FirstName and LastName fields
transformed_df = order_details.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

# Remove the CustomerName field
transformed_df = transformed_df.drop("CustomerName")
display(transformed_df.limit(5))

#The following code example saves the dataFrame into a parquet file in the data lake, replacing any existing file of the same name.
transformed_df.write.mode("overwrite").parquet('/transformed_data/orders.parquet')
print ("Transformed data saved!")