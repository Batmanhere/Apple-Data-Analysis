# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast,collect_set, size, array_contains

class Transformer:
    def __init__(self):
        pass

    def transform(self):
        pass
    
class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self,inputdfs):
        
        transactioninputdf = inputdfs.get("transactioninputdf")

        print("transactioninputdf in transform classs")

        transactioninputdf.show()

        windowspec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactioninputdf.withColumn(
            "nextProductName",lead("product_name").over(windowspec)
        )

        print("airpods after buying iphone")
        transformedDF.orderBy("customer_id","transaction_date").show()

        filteredDf = transformedDF.filter(( col("product_name") == "iPhone") & (col('nextProductName') == "AirPods")
        )

        filteredDf.show()

        customerinputdf = inputdfs.get("customerinputdf")

        joindf = customerinputdf.join(
            broadcast(filteredDf),"customer_id")
        print("after join")
        joindf.select("customer_id","transaction_date","customer_name","location").show()

        return joindf.select(
            "customer_id",
            "customer_name",
            "location"
        )
class OnlyIphoneandAirpods(Transformer):
    def transform(self,inputdfs):
        
        transactioninputdf = inputdfs.get("transactioninputdf")

        print("transactioninputdf in transform classs")

        groupedDF = transactioninputdf.groupBy("customer_id").agg(collect_set("product_name").alias("products")
        )
        print("Grouped DF")
        groupedDF.show()
        filterdf = groupedDF.filter(
            (array_contains(col("products"), "iPhone"))&
            (array_contains(col("products"), "AirPods"))&
            (size(col("products")) == 2))
        
        print("only airpods after iphone")
        filterdf.show()

        customerinputdf = inputdfs.get("customerinputdf")

        joindf = customerinputdf.join(
            broadcast(filterdf),"customer_id")
        print("after join")
        joindf.select("customer_id","customer_name","location").show()

        return joindf.select(
            "customer_id",
            "customer_name",
            "location"
        )

