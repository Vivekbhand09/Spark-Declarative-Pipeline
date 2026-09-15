# from pyspark import pipelines as dp
# from pyspark.sql.functions import * 

# # Materialized Sales 
# @dp.table(name="src_sales_stream")
# def src_sales():
#     df=spark.readStream.table("sdp_catalog.source.sales")
#     df=df.withColumn("sale_date",to_date(col("date"),"MM-dd-yyyy"))
#     return df

# # Materialized View (Referring to anothere view )
# @dp.table(name="enr_sales_stream")
# def enr_sales():
#     df=spark.readStream.table("sdp_catalog.target.src_sales_stream")
#     df=df.withColumn("revenue",col("revenue")*1.5)
#     return df

# # Materialized View (Referring to anothere view )
# @dp.table(name="cur_sales_stream")
# def cur_sales():
#     df=spark.readStream.table("sdp_catalog.target.enr_sales_stream")
#     df=df.groupBy("date").agg(sum("revenue").alias("total_sales"))
#     return df
