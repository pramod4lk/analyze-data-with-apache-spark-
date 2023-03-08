from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from matplotlib import pyplot as plt

# create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV") \
    .getOrCreate()

# define an explicit schema for the dataframe
orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
     ])

# read the CSV file
df = spark.read.load('2019.csv',
        format('csv'),
        header=False,
        inferSchema=True,
        schema=orderSchema
    )

# show the dataframe
df.show()

# print the dataframe schema
df.printSchema()

# filter the dataframe
customers = df.select('CustomerName', 'Email') #.where(df['Item'] == 'Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())
customers.distinct().show()

# aggregate and group data in a dataframe
productSales = df.select('Item', 'Quantity').groupBy('Item').sum()
productSales.show()

yearlySales = df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
yearlySales.show()

# Query data using Spark SQL
df.createOrReplaceTempView("salesorders")

spark_df = spark.sql("SELECT * FROM salesorders")
spark_df.show()

spark_groupby = spark.sql('SELECT YEAR(OrderDate) AS OrderYear, SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue FROM salesorders GROUP BY YEAR(OrderDate) ORDER BY OrderYear;')
spark_groupby.show()

# Visualize data with Spark
sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                 SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
             FROM salesorders \
             GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
             ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_spark.show()

# matplotlib requires a Pandas dataframe, not a Spark one
df_sales = df_spark.toPandas()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

# Display the plot
plt.show()

# Clear the plot area
plt.clf()

# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))

# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')

# Create a pie chart of yearly order counts on the second axis
yearly_counts = df_sales['OrderYear'].value_counts()
ax[1].pie(yearly_counts)
ax[1].set_title('Orders per Year')
ax[1].legend(yearly_counts.keys().tolist())

# Add a title to the Figure
fig.suptitle('Sales Data')

# Show the figure
plt.show()

# Using seaborn
 
# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()