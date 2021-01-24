# Databricks notebook source
# MAGIC %sh 
# MAGIC pip install --upgrade pip
# MAGIC pip3 install wget 
# MAGIC #pip3 to install for python 3

# COMMAND ----------

# installing fbProphet library changes the logging config, hence update the logger config
import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

import wget
url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'

# COMMAND ----------

# DBTITLE 1,Load Unemployment Insurance (UI) Claims data (from 1987 to April 18 2020) of US states
# MAGIC %sh
# MAGIC wget 'https://raw.githubusercontent.com/Roopana/CovidAnalysis/master/data/State_UI_claims_allstates_1987_Apr182020.csv'
# MAGIC mkdir coviddata
# MAGIC mv *.csv coviddata/
# MAGIC ls coviddata

# COMMAND ----------

# define path to file
filepath = 'file:/databricks/driver/coviddata/State_UI_claims_allstates_1987_Apr182020.csv'

# load CSV data using sqlContext 
#Not Seasonally adjusted unemployment claim data
unempClaimData = sqlContext.read.format("csv")\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load(filepath)

display(unempClaimData)

# COMMAND ----------

# DBTITLE 1,Create DELTA table of UI Claims and partition by State for query optimization
from pyspark.sql.functions import col

unempClaimData = unempClaimData.select(col("State"), col('Filed week ended').alias("Filed_week_ended"), col("Initial Claims").alias("Initial_Claims"), col("Reflecting Week Ended").alias("Reflecting_Week_Ended"), col("Continued Claims").alias("Continued_Claims"), col("Covered Employment").alias("Covered_Employment"), col("Insured Unemployment Rate").alias("Insured_Unemployment_Rate"))

dbutils.fs.rm("dbfs:/FileStore/tables/unempClaimData_delta", True)
spark.sql("DROP TABLE IF EXISTS unempClaimData_delta")

unempClaimData.write.partitionBy("State").format("delta").save("/FileStore/tables/unempClaimData_delta")
spark.sql("CREATE TABLE unempClaimData_delta USING DELTA LOCATION '/FileStore/tables/unempClaimData_delta/'")

spark.sql("SELECT * from unempClaimData_delta").show(5)
# display data in table format

# COMMAND ----------

unempClaimData_partd = spark.sql("SELECT * from unempClaimData_delta")

# COMMAND ----------

# DBTITLE 1,Data cleaning
import re
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

# few numbers have commas in their values, remove them
commaRep = UserDefinedFunction(lambda x: re.sub(',','',str(x)), StringType())
unempClaimData_partd_c = unempClaimData_partd.select(*[commaRep(column).alias(column) for column in unempClaimData_partd.columns])

from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime

unempClaimData_partd_cleaned = unempClaimData_partd_c.select('State', 'Filed_week_ended', 'Initial_Claims', 'Reflecting_Week_Ended', 'Continued_Claims', 'Covered_Employment', 'Insured_Unemployment_Rate', from_unixtime(unix_timestamp('Filed_week_ended', 'MM/dd/yyy')).alias('Filed_week_ended_fixed'), from_unixtime(unix_timestamp('Reflecting_Week_Ended', 'MM/dd/yyy')).alias('Reflecting_Week_Ended_fixed'))

unempClaimData_partd_cleaned.show(3)

# COMMAND ----------

# DBTITLE 1,create final view for partioned and cleaned UI claims data
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType
from pyspark.sql.functions import col, to_date

unempClaimData_final = unempClaimData_partd_cleaned.select( col('state'), to_date(col('Filed_week_ended_fixed')).alias('date'), col('Initial_Claims').cast('Long'), to_date(col('Reflecting_Week_Ended_fixed')), col('Continued_Claims').cast('Long'), col('Covered_Employment').cast('Long'), col('Insured_Unemployment_Rate').cast('Double') )
unempClaimData_final.repartition("state")
unempClaimData_final.show(5)
unempClaimData_final.createOrReplaceTempView("unemp_table")

maxClaimDate = spark.sql("SELECT MAX(date) FROM unemp_table").collect()[0][0]
print("UI Claim data is available till: "+ str(maxClaimDate))
# the unemployment claims data is available till 2020-04-18

# COMMAND ----------

# DBTITLE 1,Plot state wise UI claims in 2020(Result #4 in the report)
# MAGIC %matplotlib inline
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC unempClaim2020 = unempClaimData_final.where(col("date")>= "2020-01-01").toPandas()
# MAGIC 
# MAGIC unempClaim2020.set_index('date', inplace=True)
# MAGIC unempClaim2020.groupby('state')['Initial_Claims'].plot(figsize=(15,7), rot = 90, title='UI cliams across US states in 2020')
# MAGIC plt.legend(bbox_to_anchor=(1.05,1))
# MAGIC plt.show()

# COMMAND ----------

# DBTITLE 1,Display UI claims in 2020 (top 20 states)
display(spark.sql("SELECT state, SUM(Initial_Claims), date FROM unemp_table where date> '2020-03-01' GROUP BY state, date ORDER BY date, sum(Initial_Claims) DESC"))

# COMMAND ----------

# DBTITLE 1,Get Statistics on UI claims by states (Result #4 in the report)
result3 = spark.sql("SELECT state, SUM(Initial_Claims) as Initial_Claims, sum(Insured_Unemployment_Rate) as Insured_Unemployment_Rate FROM unemp_table where date> '2020-03-01' GROUP BY state ORDER BY SUM(Initial_Claims) DESC")
result3.describe(['Initial_Claims', 'Insured_Unemployment_Rate']).show()

result3.createOrReplaceTempView("result3")

max_Initial_Claims_State = spark.sql("SELECT * FROM result3 where Initial_Claims == (SELECT MAX(Initial_Claims) FROM result3) ").collect()
print(max_Initial_Claims_State)
min_Initial_Claims_State = spark.sql("SELECT * FROM result3 where Initial_Claims == (SELECT MIN(Initial_Claims) FROM result3) ").collect()
print(min_Initial_Claims_State)
max_IUR_State = spark.sql("SELECT * FROM result3 where Insured_Unemployment_Rate == (SELECT MAX(Insured_Unemployment_Rate) FROM result3) ").collect()
print(max_IUR_State)
min_IUR_State = spark.sql("SELECT * FROM result3 where Insured_Unemployment_Rate == (SELECT MIN(Insured_Unemployment_Rate) FROM result3) ").collect()
print(min_IUR_State)

# COMMAND ----------

# DBTITLE 1,Download COVID19 US state wise data
# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv
# MAGIC 
# MAGIC mv *.csv coviddata/
# MAGIC ls coviddata

# COMMAND ----------

# DBTITLE 1,Load COVID state wise data
filepath = 'file:/databricks/driver/coviddata/us-states.csv'

#Schema to load data
covidStatesSchema = StructType([StructField("date", DateType(), nullable= True), StructField("state", StringType(), True), StructField("fips", IntegerType(), True),StructField("cases", LongType(), True), StructField("deaths", LongType(), True)])
                                
# load CSV data based on given schema
covid_states_raw = sqlContext.read.format("csv")\
      .option("header", "true").schema(covidStatesSchema).load(filepath)
covid_states_raw.show(5)
covid_states_raw.describe()

# COMMAND ----------

# DBTITLE 1,Get Population Data of US States
# MAGIC %sh
# MAGIC rm coviddata/emp_civilian_nonInstPop_state_1976_2020.csv
# MAGIC wget https://github.com/Roopana/CovidAnalysis/raw/master/data/emp_civilian_nonInstPop_states_1976_2020.csv
# MAGIC mv *.csv coviddata/
# MAGIC ls coviddata

# COMMAND ----------

# DBTITLE 1,Load Population Data
# define path to file
filepath = 'file:/databricks/driver/coviddata/emp_civilian_nonInstPop_states_1976_2020.csv'

unempPopData = sqlContext.read.format("csv")\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load(filepath)
#Rename columns appropriately
unempPopData = unempPopData.select(col("FIPS Code").alias("FIPS_Code"), col('State and area').alias("State_and_area"), col("Year"), col("Month"), col("Civilian non-institutional population").alias("population"), col("Total"), col("Percent of population").alias("Percent_of_population"), col("Total Employment ").alias("Total_Employment"), col("Employment As Percent of population").alias("Employment_As_Percent_of_population"), col("Total Unemployment").alias("Total_Unemployment"), col("Unemployment Rate").alias("Unemployment_Rate"))
# display data in table format
display(unempPopData)

# COMMAND ----------

# DBTITLE 1,Create DELTA Table for population
dbutils.fs.rm("dbfs:/FileStore/tables/unempPopData_delta", True)
spark.sql("DROP TABLE IF EXISTS unempPopData_delta")

unempPopData.write.partitionBy("State_and_area").format("delta").save("/FileStore/tables/unempPopData_delta")
spark.sql("CREATE TABLE unempPopData_delta USING DELTA LOCATION '/FileStore/tables/unempPopData_delta/'")

spark.sql("SELECT * from unempPopData_delta").show(5)

# COMMAND ----------

# DBTITLE 1,Extrapolate Population Data for April and May 2020
# Population data is available only till March 2002 while UI claim data and COVID is till April, May 2020. Hence extrapolate population data

from pyspark.sql.functions import lit
unempPopData = spark.sql("SELECT * from unempPopData_delta")
popDataApr2020 = unempPopData.where(col("Year") == 2020).where(col("Month") ==3).withColumn("Month", lit(4))
popDataMay2020 = unempPopData.where(col("Year") == 2020).where(col("Month") ==3).withColumn("Month", lit(5))
popDataApr2020 = popDataApr2020.unionAll(popDataMay2020)

#append April and May data to available population data
unempPopData = unempPopData.unionAll(popDataApr2020)
unempPopData.show(5)

# COMMAND ----------

# DBTITLE 1,Calculate UI claims as % of population
from pyspark.sql.functions import year, month, expr

joinExpression = [year(unempClaimData_final['date']) == unempPopData['Year'] , month(unempClaimData_final['date']) == unempPopData['Month'] , unempClaimData_final['state'] == unempPopData['State_and_area']]

unempClaimData_joined = unempClaimData_final.join(unempPopData, joinExpression, 'left_outer').select("state", "date", "Initial_Claims", "Continued_Claims", col("population"), col("Total_Employment"), col("Total_Unemployment"), (col("Initial_Claims")*100/col("population")).alias("Inital_Claims_as_%_of_population"), (col("Continued_Claims")*100/col("population")).alias("Continued_Claims_as_%_of_population"), (col("Total_Employment")*100/col("population")).alias("Employment_Rate"))

display(unempClaimData_joined)

# COMMAND ----------

# DBTITLE 1,Time series graph of UI claims in a given state from 1987- 2020 
from pyspark.sql.functions import concat, lit, format_string

unemp_delta_NJ = unempClaimData_joined.where(col("state") == 'New Jersey').withColumn("month", concat( year(col("date")), lit("-"), format_string("%02d", month(col("date"))) ))
unemp_delta_NJ.createOrReplaceTempView("unemp_delta_NJ_view")

#aggregat UI claims by month
unemp_delta_NJ_month = spark.sql("SELECT to_date(month) as month, sum(Initial_Claims)as monthly_ui_claims, first(state) as state from unemp_delta_NJ_view group by month order by month")
display(unemp_delta_NJ_month)

# COMMAND ----------

# DBTITLE 1,Monthly UI claim data aggregated over all states since 1987 
from pyspark.sql.functions import concat, lit, format_string

unemp_delta_all_states = unempClaimData_joined.withColumn("year_month", concat( year(col("date")), lit("-"), format_string("%02d", month(col("date"))) ))
unemp_delta_all_states.createOrReplaceTempView("unemp_delta_all_states_view")

#aggregat UI claims by year_month
unemp_delta_all_states_month = spark.sql("SELECT to_date(year_month) as year_month, sum(Initial_Claims)as monthly_ui_claims from unemp_delta_all_states_view group by year_month order by year_month")
display(unemp_delta_all_states_month)

# COMMAND ----------

# DBTITLE 1,Plot Monthly UI claims data aggregated over all states since 1987(Result #3 in the report)
# MAGIC %matplotlib inline
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC unemp_delta_all_states_months_pandas = unemp_delta_all_states_month.toPandas()
# MAGIC 
# MAGIC unemp_delta_all_states_months_pandas.set_index('year_month', inplace=True)
# MAGIC unemp_delta_all_states_months_pandas['monthly_ui_claims'].plot(figsize=(20,7), title='#Monthly UI claim data over all states in US', kind = 'line', fontsize = 10)
# MAGIC plt.legend(bbox_to_anchor=(1.05,1))
# MAGIC plt.show()

# COMMAND ----------

# DBTITLE 1,Calculate COVID cases as % of population
from pyspark.sql.functions import year, month, expr

joinExpression1 = [year(covid_states_raw['date']) == unempPopData['Year'] , month(covid_states_raw['date']) == unempPopData['Month'] , covid_states_raw['state'] == unempPopData['State_and_area']]

covid_states_pop = covid_states_raw.join(unempPopData, joinExpression1, 'left_outer').select("date", "state", "cases", "deaths", (col("cases")*100/col("population")).alias("cases_as_%_of_population"), (col("deaths")*100/col("population")).alias("deaths_as_%_of_population"))

covid_states_pop.show(5)

# COMMAND ----------

#Partition data by state
covid_states_pop.repartition("state")
print("No of partitions after partitioning: "+ str(unempClaimData_partd.rdd.getNumPartitions()))

# COMMAND ----------

# DBTITLE 1,Create DELTA table for COVID data joined with Population
result2 = covid_states_pop.select("date", "state", "cases", "deaths", col("cases_as_%_of_population").alias("cases_as_p_of_population"), col("deaths_as_%_of_population").alias("deaths_as_p_of_population"))

dbutils.fs.rm("dbfs:/FileStore/tables/covidPop_delta", True)
spark.sql("DROP TABLE IF EXISTS covidPop_delta")

result2.write.format("delta").save("/FileStore/tables/covidPop_delta")
spark.sql("CREATE TABLE covidPop_delta USING DELTA LOCATION '/FileStore/tables/covidPop_delta/'")

spark.sql("SELECT * from covidPop_delta").show(5)

# COMMAND ----------

# DBTITLE 1,State wise statistics on COVID cases (Result #2 in the report)
print(spark.sql("SELECT * FROM covidPop_delta where cases == (SELECT MAX(cases) from covidPop_delta where date == (SELECT MAX(date) from covidPop_delta))").collect())
print(spark.sql("SELECT * FROM covidPop_delta where cases == (SELECT MIN(cases) from covidPop_delta where date == (SELECT MAX(date) from covidPop_delta)) and date == (SELECT MAX(date) from covidPop_delta)").collect())

# COMMAND ----------

# DBTITLE 1,Plot State wise trends of COVID19 cases
# MAGIC %matplotlib inline
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC result2P = result2.toPandas()
# MAGIC 
# MAGIC result2P.set_index('date', inplace=True)
# MAGIC result2P.groupby('state')['cases'].plot(figsize=(15,7), rot = 90, title='cases across US states in 2020')
# MAGIC plt.legend(bbox_to_anchor=(1.05,1))
# MAGIC plt.show()

# COMMAND ----------

# DBTITLE 1,Aggregate COVID data over all states
covid_all_states = covid_states_pop.sort("date").groupby(col("date")).sum().select(col("date"), col("sum(cases)").alias("cases"), col("sum(deaths)").alias("deaths"), col("sum(cases_as_%_of_population)").alias("cases_as_%_of_population"), col("sum(deaths_as_%_of_population)").alias("deaths_as_%_of_population"))
display(covid_all_states)

# COMMAND ----------

# DBTITLE 1,Result #1 in the report - Daily COVID cases aggregated over all states- Statistics
covid_all_states.describe().show()

# COMMAND ----------

# DBTITLE 1,Plot Aggregated COVID data over all states
# MAGIC %matplotlib inline
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC covid_all_states_pandas = covid_all_states.toPandas()
# MAGIC 
# MAGIC covid_all_states_pandas.set_index('date', inplace=True)
# MAGIC covid_all_states_pandas['cases'].plot(figsize=(18,7), rot = 90, title='# COVID19 Cases aggregated over all states in US', kind = 'bar', fontsize = 8)
# MAGIC plt.legend(bbox_to_anchor=(1.05,1))
# MAGIC plt.show()

# COMMAND ----------

# DBTITLE 1,Join UI claims data and COVID state wise data on week of the year 2020
from pyspark.sql.functions import weekofyear

covid_states_week = covid_states_pop.withColumn("week_of_year", weekofyear("date"))
# Aggregate cases for each week in 2020
covid_states_week_sum = covid_states_week.groupby("week_of_year", "state").sum().select(col("week_of_year"), col("state"), col("sum(cases)").alias("cases"), col("sum(deaths)").alias("deaths"), col("sum(cases_as_%_of_population)").alias("cases_as_%_of_population"), col("sum(deaths_as_%_of_population)").alias("deaths_as_%_of_population"))

unempClaimData_2020 = unempClaimData_joined.where(col("date")>= '2020-01-01').withColumn("week_of_year", weekofyear("date"))

covid_unemp_2020 = unempClaimData_2020.join(covid_states_week_sum,['state','week_of_year'],'left_outer')
# this df has null values for #cases, #deaths in the initial week sof 2020. Fill them with 0
covid_unemp_2020 = covid_unemp_2020.na.fill(0)

dbutils.fs.rm("dbfs:/FileStore/tables/covid_unemp_2020_delta", True)
spark.sql("DROP TABLE IF EXISTS covid_unemp_2020_delta")

covid_unemp_2020.write.format("delta").save("/FileStore/tables/covid_unemp_2020_delta")
spark.sql("CREATE TABLE covid_unemp_2020_delta USING DELTA LOCATION '/FileStore/tables/covid_unemp_2020_delta'")

spark.sql("SELECT * from covid_unemp_2020_delta").show(5)

# COMMAND ----------

covid_unemp_2020_agg = spark.sql("SELECT week_of_year, sum(Initial_Claims) as Initial_Claims, sum(cases) as cases, sum(deaths) as deaths, first(date) as date from covid_unemp_2020_delta GROUP BY week_of_year ORDER BY week_of_year")
covid_unemp_2020_agg.show()

# COMMAND ----------

# DBTITLE 1,Plot Cases Vs UI claims
# MAGIC %matplotlib inline
# MAGIC import matplotlib.pyplot as plt
# MAGIC covid_unemp_agg_all_states = covid_unemp_2020.sort("date").groupby(col("date")).sum().select(col("date"), col("sum(Initial_Claims)").alias("Initial_Claims"), col("sum(cases)").alias("cases"))
# MAGIC display(covid_unemp_agg_all_states)
# MAGIC covid_unemp_agg_all_states_pandas = covid_unemp_agg_all_states.toPandas()
# MAGIC 
# MAGIC covid_unemp_agg_all_states_pandas.set_index('cases', inplace=True)
# MAGIC covid_unemp_agg_all_states_pandas['Initial_Claims'].plot(figsize=(12,7), title='#UI claim data over all states in US', kind = 'line', fontsize = 1)
# MAGIC plt.legend(bbox_to_anchor=(1.05,1))
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pystan
# MAGIC pip install https://github.com/stan-dev/pystan/archive/v2.18.0.0-cvodes.tar.gz
# MAGIC pip install fbprophet

# COMMAND ----------

# DBTITLE 1,Prophet model to fit UI claims Time series data (Result #6, #7 in the report)
from fbprophet import Prophet

# instantiate the model and set parameters
model = Prophet(
    interval_width= 1,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
)
# fit the model to historical data
formatteddf = unemp_delta_all_states_month.select(col("year_month").alias("ds"), col("monthly_ui_claims").alias("y")).toPandas()
model.fit(formatteddf)

# COMMAND ----------

# DBTITLE 1,Calculate UI claims - future predictions
future_pd = model.make_future_dataframe(
  periods=24, 
  freq='m', 
  include_history=True
  )
# predict over the dataset
forecast_pd = model.predict(future_pd)
trends_fig = model.plot_components(forecast_pd)
display(trends_fig)

# COMMAND ----------

# DBTITLE 1,Time series Analysis of COVID cases in US (Result #5 in the report)
from fbprophet import Prophet
# instantiate the model and set parameters
covid_model = Prophet(
    interval_width= 1,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=False,
    seasonality_mode='multiplicative'
)
# fit the model to historical data
#covid_states_pop_NJ = covid_states_pop.where(col("state") == "New Jersey")
formatteddf = covid_all_states.select(col("date").alias("ds"), col("cases").alias("y")).toPandas()
covid_model.fit(formatteddf)

future_pd = covid_model.make_future_dataframe(
  periods=130, 
  freq='d', 
  include_history=True
  )
# predict over the dataset
forecast_pd = covid_model.predict(future_pd)
trends_fig = covid_model.plot_components(forecast_pd)
display(trends_fig)

# COMMAND ----------

# DBTITLE 1,Analysis of UI claims using Covid Cases as an additional regressor (Result#8 in the report)
import pandas as pd
from fbprophet import Prophet
from pyspark.sql.functions import col 
formatteddf = covid_unemp_2020_agg.select(col("date").alias("ds"), col("cases").alias("y"), col("Initial_Claims").alias("z")).toPandas()
# data is weekly: weekly_seasonality=True, data available for only 1 year:  yearly_seasonality= False,
m = Prophet(daily_seasonality = False, yearly_seasonality= False, weekly_seasonality=True, interval_width=0.95, n_changepoints= 10)
#INFO:fbprophet:n_changepoints greater than number of observations. Using 11.
m.fit(formatteddf)
future = m.make_future_dataframe(periods=30, freq='W')
forecast = m.predict(future)

#Add additional regressor
dff = formatteddf.rename(columns={'y':'causal','z':'y'})
# data is weekly, for multiple years hence daily_seasonality = False,yearly_seasonality= True, weekly_seasonality=True
p = Prophet(daily_seasonality = False,yearly_seasonality= True, weekly_seasonality=True)
p.add_regressor('causal')
p.fit(dff)
future1 = m.make_future_dataframe(periods=80)

kk = forecast['yhat']
kk = pd.DataFrame(kk,columns=['y'])
z = pd.concat([pd.DataFrame(formatteddf['y']),kk],ignore_index=True)

display(future1)

future1['causal']=z

future1 = future1.fillna(0)
forecast1 = p.predict(future1)
fig = m.plot_components(forecast1)
display(fig)
