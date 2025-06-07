# Databricks notebook source
# MAGIC %run "./Reader_factory"

# COMMAND ----------

class Extractor:
    def __init__(self):
        pass

    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):

        transactioninputdf = get_data_source(
            datatype = "csv",
            file_path="dbfs:/FileStore/Transaction_Updated.csv"
        ).get_data_frame()

        transactioninputdf.orderBy("customer_id","transaction_date").show()

        customerinputdf = get_data_source(
            datatype = "delta",
          file_path="default.customer_delta_table_persistent6"
        ).get_data_frame()
        print("customer data")
        customerinputdf.show()

        inputdfs = {
            "transactioninputdf" : transactioninputdf,
              "customerinputdf" : customerinputdf  
        }
    
        return inputdfs