%spark.pyspark from pyspark.sql import functions as F
users = spark.createDataFrame(
[ ("u1", "Berlin"),
("u2", "Berlin"),
("u3", "Munich"),
("u4", "Hamburg"), ],
["user_id", "city"] )

orders = spark.createDataFrame(
[ ("o1", "u1", "p1", 2, 10.0),
("o2", "u1", "p2", 1, 30.0),
("o3", "u2", "p1", 1, 10.0),
("o4", "u2", "p3", 5, 7.0),
("o5", "u3", "p2", 3, 30.0),
("o6", "u3", "p3", 1, 7.0),
("o7", "u4", "p1", 10, 10.0), ],
["order_id", "user_id", "product_id", "qty", "price"] )

products = spark.createDataFrame(
[ ("p1", "Ring VOLA"),
("p2", "Ring POROG"),
("p3", "Ring TISHINA"), ],
["product_id", "product_name"] )

users.show() 
orders.show() 

%spark.pyspark
products.show()

%spark.pyspark
df_joined = orders.join(users, on="user_id", how="left") \
                  .join(products, on="product_id", how="left") \
                  .withColumn("revenue", F.col("qty") * F.col("price"))

%spark.pyspark
df_joined.show()

%spark.pyspark
df_agg = df_joined.groupBy("city", "product_id", "product_name") \
    .agg(
        F.count("order_id").alias("orders_cnt"),
        F.sum("qty").alias("qty_sum"),
        F.sum("revenue").alias("revenue_sum")
    )

df_agg.show()


%spark.pyspark
from pyspark.sql.window import Window


window_spec = Window.partitionBy("city").orderBy(F.col("revenue_sum").desc())

df_top_products = df_agg.withColumn("rn", F.row_number().over(window_spec)) \
                        .filter(F.col("rn") <= 2) \
                        .drop("rn")


df_top_products.show()


%spark.pyspark
output_path = "/tmp/sandbox_zeppelin/mart_city_top_products/"

df_top_products.write \
    .mode("overwrite") \
    .parquet(output_path)


df_read = spark.read.parquet(output_path)

df_read.show()