# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        raise ValueError("not implemented")

class AirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        
        getSinkSource(
            sinktype = "dbfs",
            df = self.transformedDF,
            path = "dbfs:/FileStore/tables/appleanalysis/output/airpodsafteriphone",
            method = "overwrite"
        ).load_dataframe()


class OnlyIphoneandAirpodsLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionByColumns" : ["location"]
        }
        
        getSinkSource(
            sinktype = "dbfs_with_partition",
            df = self.transformedDF,
            path ="dbfs:/FileStore/tables/appleanalysis/output/airpodsonlyiphone",
            method = "overwrite",
            params = params
        ).load_dataframe()

        getSinkSource(
            sinktype = "delta",
            df = self.transformedDF,
            path ="default.onlyAirpodsAndIphone",
            method = "overwrite",
            params = params
        ).load_dataframe()