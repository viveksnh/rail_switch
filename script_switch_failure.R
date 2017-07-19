# Reading in the data from european format CSV file
library(data.table);library(sqldf)
##this works well
#Reading sensor data
#Data was in very bad format. The column headers were incomplete in the first row of csv
#To address this issue, I am using a predefined list of col.names

#max number of fields (columns) in the dataaset - num of columns vary for each row
max_cols = max(count.fields("C:/Users/vsinha/Documents/SWITCH_EVENT_SENSORDATA_LIST.csv", sep=";"))

dt_sensor = read.table("C:/Users/vsinha/Documents/SWITCH_EVENT_SENSORDATA_LIST.csv", header = FALSE, sep = ";", 
                       col.names = paste0("V",seq_len(max_cols)), fill = TRUE, dec=",", skip = 1,
                       colClasses = c("integer", "character", "numeric", "character",rep("numeric",720)))
dt_sensor = as.data.table(dt_sensor)
setnames(dt_sensor, c("V1", "V2", "V3","V4"), c("event_id", "sensor_id", "switching_time", "AllMetrics"))
dt_sensor$AllMetrics=NULL
dt_sensor[, event_id := as.integer(event_id)]

##Event Data
dt_event = as.data.table(read.csv("C:/Users/vsinha/Documents/SWITCH_USAGE_EVENT_LIST.csv", sep=";", dec = ","))
dt_event[, event_id := as.integer(event_id)]
dt_event = as.data.table(dt_event)

##Weather Data
dt_weather = as.data.table(read.csv("C:/Users/vsinha/Documents/SWITCH_WEATHER_SENSORDATA_LIST.csv", sep=";", dec = ","))
dt_weather = as.data.table(dt_weather)

#Merging all the datasets together
dt = merge(dt_event, dt_sensor, by="event_id")

#Creating a date field
dt[, event_dt:=as.Date(event_time),]

##Adding cumulative count of failures (ranked by date)
dt[, n_failure := cumsum(event_result=="failure"), by=c("switch_id", "sensor_id")]
#Adding a count of uses - resets at every failure
dt[, n_uses:=cumsum(event_result!="failure"), by=c("switch_id", "sensor_id", "n_failure")]

#Creating aggregated dataset using sqldf
##Aggregating dataset - example for creating 7 day average with 7 day blackout period
#----!!! This is running out of memory. so moving top AZURE VM at this point.
dt_temp = 
result = sqldf(
"Select dt.event_id, 
dt.switch_id,
dt.switch_type,
dt.event,
dt.event_time,
dt.event_dt,
dt.event_result,
dt.event_type,
dt.sensor_id,
dt_temp.*
from
dt
left join dt_temp
on dt_temp.event_dt between dt.event_dt-14 and dt.event_dt-7
where dt.switch_id='Kue6' and dt_temp.switch_id='Kue6'
group by dt.event_id, 
dt.switch_id,
dt.switch_type,
dt.event,
dt.event_time,
dt.event_dt,
dt.event_result,
dt.event_type,
dt.sensor_id,
dt_temp.V1")
