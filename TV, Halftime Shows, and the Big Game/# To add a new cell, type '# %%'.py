# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import scipy.stats


# %%
datakmeans = pd.concat([pd.read_csv(f) for f in glob.glob('kmeans*.csv')], ignore_index = True)
datalr = pd.concat([pd.read_csv(f) for f in glob.glob('lr*.csv')], ignore_index = True)
datasleep = pd.concat([pd.read_csv(f) for f in glob.glob('sleep*.csv')], ignore_index = True)
datasparkpi = pd.concat([pd.read_csv(f) for f in glob.glob('sparkpi*.csv')], ignore_index = True)
datasql = pd.concat([pd.read_csv(f) for f in glob.glob('sql*.csv')], ignore_index = True)


# %%
datakmeans = datakmeans.drop(["timestamp","spark.app.name",'run','saveMode','output','spark.driver.host','spark.driver.port','spark.jars','spark.app.name','spark.executor.id','spark.submit.deployMode','spark.master','spark.app.id','description','k','input'],axis=1)
datalr = datalr.drop(["timestamp","spark.app.name",'run','saveMode','output','spark.driver.host','spark.driver.port','spark.jars','spark.app.name','spark.executor.id','spark.submit.deployMode','spark.master','spark.app.id','description','input','numPartitions','numRows','numCols','eps','intercepts'],axis=1)
datasleep = datasleep.drop(["timestamp","spark.app.name",'run','saveMode','output','spark.driver.host','spark.driver.port','spark.jars','spark.app.name','spark.executor.id','spark.submit.deployMode','spark.master','spark.app.id','description','input','distributionMean','distributionMin','distributionStd'],axis=1)
datasparkpi = datasparkpi.drop(["timestamp","spark.app.name",'run','saveMode','output','spark.driver.host','spark.driver.port','spark.jars','spark.app.name','spark.executor.id','spark.submit.deployMode','spark.master','spark.app.id','description','input','slices'],axis=1)
datasql = datasql.drop(["timestamp","spark.app.name",'run','saveMode','output','spark.driver.host','spark.driver.port','spark.jars','spark.app.name','spark.executor.id','spark.submit.deployMode','spark.master','spark.app.id','description','input','cache','queryStr','numPartitions'],axis=1)


# %%
def data_means(data, list_columns):
    for column in list_columns:
        print(column, "   ", data[column].mean()/100000000)

data_means(datakmeans,["total_runtime","train"])
data_means(datalr,["total_runtime","execution_time"])
data_means(datasleep,["total_runtime","sleepMS"])
data_means(datasparkpi,["total_runtime"])
data_means(datasql,["total_runtime","queryTime"])


# %%



# %%


