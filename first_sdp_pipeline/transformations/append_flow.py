# from pyspark import pipelines as dp
# from pyspark.sql.functions import *

# # Creating empty streaming table 
# dp.create_streaming_table("total_sales")

# #Appending north sales to the "total_sales"
# @dp.append_flow(target="total_sales")
# def north_sales():
#     df=spark.readStream.table("sdp_catalog.source.north_sales")
#     return df

# #Appending south sales to the "total_sales"
# @dp.append_flow(target="total_sales")
# def south_sales():
#     df=spark.readStream.table("sdp_catalog.source.south_sales")
#     return df