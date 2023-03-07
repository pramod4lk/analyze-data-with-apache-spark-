# PySpark code to load the data into a dataframe and display the first 10 rows.

%%pyspark
df = spark.read.load('abfss://container@store.dfs.core.windows.net/products.csv',
    format='csv',
    header=True
)
display(df.limit(10))

# Scala code for the same

%%spark
val df = spark.read.format("csv").option("header", "true").load("abfss://container@store.dfs.core.windows.net/products.csv")
display(df.limit(10))


# The following PySpark example shows how to specify a schema for the dataframe
#  to be loaded from a file named product-data.csv in this format.

from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('abfss://container@store.dfs.core.windows.net/product-data.csv',
    format='csv',
    schema=productSchema,
    header=False)

display(df.limit(10))

# Following code example uses the select method to retrieve the ProductName and ListPrice columns from the df dataframe.

pricelist_df = df.select("ProductID", "ListPrice")

# this example code chains the select and where methods to create a new dataframe
# containing the ProductName and ListPrice columns for products with a category of Mountain Bikes or Road Bikes.

bikes_df = df.select("ProductName", "ListPrice").where((df["Category"]=="Mountain Bikes") | (df["Category"]=="Road Bikes"))
display(bikes_df)

# you can use the groupBy method and aggregate functions. For example,
# the following PySpark code counts the number of products for each category.

counts_df = df.select("ProductID", "Category").groupBy("Category").count()
display(counts_df)

# One of the simplest ways to make data in a dataframe available for querying
#  in the Spark catalog is to create a temporary view, as shown in the following code example:

df.createOrReplaceTempView("products")

# You can create an empty table by using 
spark.catalog.createTable

# You can save a dataframe as a table by using its saveAsTable method.

# You can create an external table by using
spark.catalog.createExternalTable

# The following PySpark code uses a SQL query to return data from the products view as a dataframe.

bikes_df = spark.sql("SELECT ProductID, ProductName, ListPrice \
                      FROM products \
                      WHERE Category IN ('Mountain Bikes', 'Road Bikes')")
display(bikes_df)

# In a notebook, you can also use the %%sql magic to run SQL code that queries objects in the catalog, like this.

%%sql
SELECT Category, COUNT(ProductID) AS ProductCount
FROM products
GROUP BY Category
ORDER BY Category