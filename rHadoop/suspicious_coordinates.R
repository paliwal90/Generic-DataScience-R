#######################################################################################################
#  suspicious_coordinates.R
#  ========================
#  1. Implements Suspicious Coordinates Heuristics
#  2. Push returned coordinates to DMP Hadoop under Cron Job (running once a day)
#
#  Author: Piyush Paliwal
#######################################################################################################

library(lmds)
library(ggplot2)
library(data.table)
library(splitstackshape)
CONSUL_DNS = 'consul-webui.service.dc1.consul:8500'


start_date=Sys.Date()
day=start_date
period=2 #3 days data 
days<-past_days(start_date,0:period)
for (day in days){
  print (day)
  ad_success<-scroll2dt(elastic_ip=lmds::get_elastic_ip(CONSUL_DNS), 
                        index=paste0("kafka-events-",day), 
                        type="ad_success", 
                        fields=c("request_id","device_id","app_id","app_type","extra.app_name","extra.ssp","country_id","extra.location"), 
                        q = "extra.location_type:gps")
}

#----------------------------------------------------------------------------------------------
#   preprocess_data 
#   ======================
#   preprocess data (remove missing coordinates, round off after decimal points, keep subset of features)
#----------------------------------------------------------------------------------------------
preprocess_data <- function(dat, round_N) {
  dat <- dat[!is.na( dat$extra.location),]
  dat <- dat[!(dat$extra.location=="NaN,NaN"),] #is.na() doesn't identify all cases
  #dat <- cSplit( dat,"extra.location",",")
  #dat$extra.location_1_rounded <- round(dat$extra.location_1,round_N)
  #dat$extra.location_2_rounded <- round(dat$extra.location_2,round_N)
  #dat$extra.location <- with(dat, paste(extra.location_1_rounded,extra.location_2_rounded, sep=","))
  #dat <- subset(dat, select = -c(extra.location_1,extra.location_2,extra.location_1_rounded,extra.location_2_rounded,country_id)) 
  return(dat)
}


#----------------------------------------------------------------------------------------------
#   get_location_by_device
#   ======================
#   get location (coordinate) with distinct device count 
#----------------------------------------------------------------------------------------------
get_location_by_device <- function(dat){
  location_by_device <- dat[, .(distinct_devices = uniqueN(device_id)), by = extra.location]
  location_by_device <-location_by_device [order(-distinct_devices)]
  location_by_device$distinct_devices_per <- (location_by_device$distinct_devices/length(unique(df$device_id)))*100
  return(location_by_device)
}

destfile <- "suspicious_coordinates/suspicious_coordinates_list.csv"

#------------------------------------------------------------------------------------------------
#   insert_heuristic
#   ================
#   1: coordinates that have shared devices > min_device are suspicious (sus.coor.latest),
#   2: add them to file (if not already on server - initially),
#   3: load the suspicious coordinate generated on day before (sus.coor.previous),
#   4: if coordinates in sus.coor.previous are not in sus.coor.latest, add them to sus.coor.latest
#   
#   returns sus.coor.latest
#-------------------------------------------------------------------------------------------------
insert_heuristic <- function(location_by_device, min_device){
  
  sus.coor.latest <- subset(location_by_device[distinct_devices>=min_device,], select = c(extra.location))
  sus.coor.latest <- cSplit(sus.coor.latest,"extra.location",",")
  colnames(sus.coor.latest)[1:2] <- c("longitude", "latitude")
  sus.coor.latest$extra.location <- with(sus.coor.latest, paste(longitude,latitude, sep=","))
  sus.coor.latest$last_flagged_on <- start_date
  
  
  if (!file.exists(destfile)) {    
    write.table(sus.coor.latest, file = destfile, row.names=FALSE, quote = FALSE, sep = "\t")
  }
  
  #this strategy is supposed to be way faster [avoid 2nd circle of hell: by not using rbind inside for-loop to grow data frame]
  sus.coor.previous <- fread(destfile,  sep = "\t")
  coor <- character(nrow(sus.coor.previous))
  x_longitude <- character(nrow(sus.coor.previous))
  x_latitude <- character(nrow(sus.coor.previous))
  y <- character(nrow(sus.coor.previous))
  j<-0
  for (i in 1:nrow(sus.coor.previous)){
    if (!sus.coor.previous$extra.location[i] %in% sus.coor.latest$extra.location) {
      j<-j+1
      coor[j]<-sus.coor.previous$extra.location[i]
      x_longitude[j]<-sus.coor.previous$longitude[i]
      x_latitude[j]<-sus.coor.previous$latitude[i]
      y[j]<-sus.coor.previous$last_flagged_on[i]
    }
  }
  
  if (j!=0){
    sus.coor.latest <- rbind(sus.coor.latest,data.frame(longitude=x_longitude[1:j],latitude=x_latitude[1:j],extra.location=coor[1:j],last_flagged_on=as.Date(y[1:j])))
  }
  
  return(sus.coor.latest)
}

#----------------------------------------------------------------------------------------------
#   delete_heuristic
#   ================
#   keep suspicious coordinates if their flagging date is not older than last 10 days
#----------------------------------------------------------------------------------------------
delete_heuristic <- function(dat,last_n_days){
  keep_after_date <- start_date - last_n_days
  dat <- dat[dat$last_flagged_on>keep_after_date,]
  return(dat)
}

#----------------------------------------------------------------------------------------------
#   write_coordinates
#   =================
#   write coordinates to server and then to dmp hadoop 
#----------------------------------------------------------------------------------------------
write_coordinates <- function(dat){
  
  message('----------- writing-to-server -----------')
  
  write.table(dat, file = destfile, row.names=FALSE, quote = FALSE, sep = "\t")
  
  Sys.setenv(JAVA_HOME='/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre') 
  
  message('----------- writing-from-server-to-hadoop -----------')
  
  system("/home/paliwal/hadoop-2.7.0/bin/hadoop fs -copyFromLocal -f /home/paliwal/suspicious_coordinates/suspicious_coordinates_list.csv hdfs://hadoop001.fal.loopmedc.com/user/piyush")
}

#----------------------------------------------------------------------------------------------
#   log_extra
#   =============
#   log apps and rtbs from where suspicious coordinates are generated
#----------------------------------------------------------------------------------------------
log_extra <- function(dat,coord){
  suspicious_requests <- subset(dat,extra.location %in% coord$extra.location)
  message('----------- writing-log-file -----------')
  write.table(unique(suspicious_requests$extra.app_name)[!is.na(unique(suspicious_requests$extra.app_name))], file = "suspicious_coordinates/suspicious_apps", row.names=FALSE, col.names=FALSE, quote = FALSE, sep = "\t")
  write.table(unique(suspicious_requests$extra.ssp)[!is.na(unique(suspicious_requests$extra.ssp))], file = "suspicious_coordinates/suspicious_rtbs", row.names=FALSE, col.names=FALSE, quote = FALSE, sep = "\t")
}

df <- preprocess_data (ad_success, 4)

location_by_device <- get_location_by_device(df)

sus.coor.latest <- insert_heuristic(location_by_device, 10)

sus.coor.latest <- delete_heuristic(sus.coor.latest, 10)

write_coordinates(sus.coor.latest)

log_extra(df, sus.coor.latest)


message('--- sus.coor: OK ---') 