# Databricks notebook source
# MAGIC %run "./Reader_factory"
# MAGIC

# COMMAND ----------

# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:

    def __init__(self):
        pass

    def runner(self):

        inputdfs = AirpodsAfterIphoneExtractor().extract()

        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputdfs)

        AirpodsAfterIphoneLoader(firstTransformedDF).sink()        
       

       
        



# COMMAND ----------

class SecondWorkFlow:
    ''' ETL pipeline to generate the data for all customers who bought both iphone and airpods  '''
    def __init__(self):
        pass

    def runner(self):

        inputdfs = AirpodsAfterIphoneExtractor().extract()

        OnlyIphoneandAirpodsDF = OnlyIphoneandAirpods().transform(inputdfs)

        OnlyIphoneandAirpodsLoader(OnlyIphoneandAirpodsDF).sink()        
       

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name
        
    def runner(self):
        if self.name== 'FirstWorkFlow':
            FirstWorkFlow().runner()
        elif self.name== 'SecondWorkFlow':
            SecondWorkFlow().runner()        
        else:
            raise ValueError(f"not implemented for {self.name}")

name = "SecondWorkFlow"

workFlowRunner = WorkFlowRunner(name).runner()


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Apple_ETL').getOrCreate()

inputdf = spark.read.format("csv").option('header','True').load("dbfs:/FileStore/Customer_Updated.csv")
inputdf.show()





# COMMAND ----------

