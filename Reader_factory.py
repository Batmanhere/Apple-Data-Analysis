# Databricks notebook source
class DataSource:

    def __init__(self,path):
        self.path=path
    
    def get_dataframe(self):

        raise ValueError("not implemented")
class CSVDataSource(DataSource):

    def get_data_frame(self):

        return(
            spark.
            read.
            format("csv").
            option('header',True).
            load(self.path)

        )
class ParquetDataSource(DataSource):

    def get_data_frame(self):

        return(
            spark.
            read.
            format("parquet").
            option('header',True).
            load(self.path)
            
        )
class deltaDataSource(DataSource):

    def get_data_frame(self):
        table_name  = self.path  
        return(
            spark.
            read.
            table(table_name)
            
        )      

def get_data_source(datatype,file_path):

    if datatype == "csv":
        return  CSVDataSource(file_path)
    elif datatype== "parquet":
        return  ParquetDataSource(file_path)
    elif datatype == "delta":
        return  deltaDataSource(file_path)
    else:
        raise ValueError(f"not implemented for datatype: {datatype}")


# COMMAND ----------

