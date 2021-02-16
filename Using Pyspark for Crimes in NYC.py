#!/usr/bin/env python
# coding: utf-8

#    ##  NYC crimes using Pyspark
#                                          
#                                                   

# In[79]:


# importing libraries:
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
get_ipython().run_line_magic('matplotlib', 'inline')
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
from random import sample
from folium.plugins import HeatMap


# create a spark session app with the name of NYC_CRIME_ANALYSIS:
spark_session = SparkSession.builder.appName("NYC_crime_analysis").getOrCreate()


# In[80]:


# read the crimes dataframe:
file = "/Users/karimaidrissi/Desktop/DSSA 5101/complaint.csv"
df = spark_session.read.csv(file, header = "True", inferSchema = "True")


# In[81]:


# checking the columns and the data types:
df.columns
df.dtypes


# In[82]:


# counting how many records in the data:
df.count()


# In[83]:


# printing the schema:
df.printSchema()


# In[84]:


# summary of the data:
df.describe().show()


# In[85]:


# calculate the statistics of some columns:

df.select(["X_COORD_CD", "Y_COORD_CD", "latitude", "longitude"]).describe().show()


# In[86]:


#  drop some columns:
df1 = df.drop("ADDR_PCT_CD", "LOC_OF_OCCUR_DESC", "PD_DESC","RPT_DT","PATROL_BORO","PD_CD","RDT_DT","X_COORD_CD","Y_COORD_CD", "lat_lon")


# In[87]:


# convert Spark data into Pandas DataFrame.
df1.toPandas()


# In[88]:


# Renaming the columns:
new_names = ("ID","Borough","Date","Time","Jurisdiction","Code","Level of offense", "Offense", "Premise" , "Suspicious age", "Suspicious race", "Suspicious sex", "Victim age", "Victim race", "Victim sex","Latitude", "Longitude")
df2 = df1.toDF(*new_names)


# In[89]:


# showing only df2 columns:
df2.columns


# In[90]:


# counting how many Boroughs in the data :
df2.select("Borough").distinct().count()


# In[91]:


# counting how many offenses:
df2.select("Offense").distinct().count()


# In[92]:


# showing only 20 offenses in our data
df2.select("Offense").distinct().show(n =20)


# In[93]:


# counting how many Harrassment 2 in the dataset:
df2.where(df2["Offense"] == "HARRASSMENT 2").count()


# In[94]:


df2.columns


# In[95]:


# filter Date bw 1/1/19 and 6/31/19:
#d = df2[(df2['Date'] >= '01/01/19') & (df2['Date'] <= '06/31/19')]
#d.show()


# In[96]:


# split the date column into year, month and day :
split_date = F.split(df2['Date'], "/")

df2 = df2.withColumn('Year', split_date.getItem(2))
df2 = df2.withColumn('Month', split_date.getItem(0))
df2 = df2.withColumn('Day', split_date.getItem(1))


# In[97]:


df2.columns


# In[98]:


df2.select('Year', 'Month', 'Day').show()


# In[99]:


# counting total number of crimes per month:
count_month =df2.groupBy(['Month']).count().filter("`count`>3").sort('count', ascending = False).toPandas()
count_month


# In[100]:


# total number of crimes for each month using bar graph:
count_month.plot(kind = "bar" , x = "Month", y ="count", fontsize =13, title = "Number of Crime Per Month")


# May is the highest crime month during the first 6 month of 2019


# In[101]:


# line graph of number of crimes for each month :
count_month.plot(kind = "line" , x = "Month", y ="count")


# In[102]:


# counting which days have the highest crimes number:
count_day = df2.groupBy('Day').count().sort("Day", ascending = True).toPandas()
count_day


# In[103]:


count_day.plot(kind = "line", x = "Day",y = "count", color = "green")
plt.xlabel("Days")
plt.ylabel("Number of Crimes")
plt.title("Number of Crimes per Day")


# In[104]:


# We can use SQL query to interact with spark DataFrame:
# first we will make a temporary table called Crimes 

df2.createOrReplaceTempView("Crimes")
avg_month = spark_session.sql("""
    SELECT Month, COUNT(*) AS Total
    FROM Crimes
    WHERE Month BETWEEN '1' AND '6'
    GROUP BY Month 
    ORDER BY Total DESC
""")
avg_month = avg_month.toPandas()
avg_month


# In[105]:


# Crimes Count for each month by using sparkSQL: 
avg_month.plot(kind = "bar" , x = "Month", y ="Total", fontsize =13, title = "Number of Crime Per Month")


# In[106]:


# Counting the crimes for each Borough:

boro_count=df2.groupBy(["Borough"]).count().sort("count", ascending = False).toPandas()
boro_count


# In[107]:


# plottinh bar graph counting the number of crimes per Borough:
boro_count.plot(kind="bar", x ="Borough",color = "green", y = "count", figsize =(10,10))

plt.xlabel("Boroughs Name", fontsize = 15)
plt.ylabel("Number of Crimes" , fontsize = 15)
plt.title("Counting the number of Crimes for each Borough", fontsize = 20)


#BROKLYN has the most crime complaints during 2019, followed by Manhattan


# In[108]:


# counting the level of offense per Borough:

off_boro = df2.groupBy(["Borough", "Level of offense"]).count().toPandas()
off_boro


# In[109]:


# counting the offenses and filter the count:
offense= df2.groupBy(["offense"]).count().filter("`count`>10").sort('count', ascending = True).toPandas()
offense


# In[110]:


# plotting the number of crimes per offenses type:
offense.plot(kind = "barh", x = "offense", y = "count", figsize = (10,30))
plt.xlabel("Number of offenses", fontsize = 10)
plt.ylabel("Offense Type", fontsize = 10)
plt.title("Counting the number of offenses", fontsize = 20)


# PETIT LARCENY is the most frequent offense during 2019.


# In[111]:


# counting the number of crimes by premises :
premises = df2.groupBy("premise"). count().sort( 'count', ascending = True).toPandas()
premises


# In[115]:


premises.plot(kind = "barh", x = "premise", y = "count", figsize = (10,20), title = "Counting the number of crimes by Premises")
plt.xlabel("Number of crimes", fontsize = 15)
plt.ylabel("Crimes location", fontsize = 15)

# The most crimes occurred in street followed by Residence in apartement or house.


# In[119]:


df2 = df2.toPandas()
df2


# In[120]:


# plotting the age group the most attacked during 2019:


sns.catplot(x = "Victim age", kind = "count", data = df2, height =10, aspect =1.4)


# seems like the victim age is between 25 and 44


# In[121]:


# plotting the suspicious age:

sns.catplot(x = "Suspicious race", kind = "count", data = df2, height =10, aspect =1.4)

# the black are more suspected than others


# In[122]:


import folium
import pandas as pd
map2 = folium.Map(location=[40.712776, -74.005974], tiles ="cartodbdark_matter", zoom_start =10)
map2


# In[123]:


# plotting a map of the most crimes in NYC by using Latitude and longtitude.

# first I will drop NA from the data
d = df2.dropna()

positions = list(zip(d['Latitude'], d['Longitude']))
map1 = folium.Map(location=[40.712776, -74.005974], zoom_start=10, tiles = "cartodbdark_matter")
HeatMap(positions[:30000], radius = 10).add_to(map1) 
map1


# by looking at the map , we can confirm that Manhattan and Bronx have more crimes during the first 6 month of 2019.


# In[ ]:




