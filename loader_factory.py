# Databricks notebook source
class DataSink:

    def __init__(self,df,path,method,params):
        self.path=path
        self.method=method
        self.params = params
        self.df = df
    
    def load_dataframe(self,path,method):

        raise ValueError("not implemented")

class LoadToDBFS(DataSink):

    def load_dataframe(self):

        self.df.write.mode(self.method).save(self.path)
        print("Loading completed")

class LoadToDBFSwithPartition(DataSink):

    def load_dataframe(self):

       partitionByColumns = self.params.get("partitionByColumns")
       self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

       print("Loading completed")

class LoadToDeltaTable(DataSink):

    def load_dataframe(self):
        
        
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)
        print("Loading completed")
def getSinkSource(sinktype,df, path, method, params = None):
    if sinktype == "dbfs":
        return LoadToDBFS(df,path,method,params)
    elif sinktype =="dbfs_with_partition":
        return LoadToDBFSwithPartition(df,path,method,params)
    elif sinktype =="delta":
        return LoadToDeltaTable(df,path,method,params)
    else:
        return ValueError(f"not implemented for sink {sinktype}")



# COMMAND ----------

