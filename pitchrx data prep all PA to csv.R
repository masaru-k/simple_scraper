library(sqldf)
library(purrr)
library(tidyverse)
library(lubridate)
library(mlbgameday)

#working directory
setwd("D:/baseball/MLB/pitchrx")
library(pitchRx)


#create database and scrape - set create = T for new DB, create = F if you have already run
db <- src_sqlite("pitchfx.sqlite3", create = T)

scrape(start = "2013-02-15", end = "2013-11-15", connect = db$con)
scrape(start = "2014-02-15", end = "2014-11-15", connect = db$con)
scrape(start = "2015-02-15", end = "2015-11-15", connect = db$con)
scrape(start = "2016-02-15", end = "2016-11-15", connect = db$con)
scrape(start = "2017-02-15", end = "2017-11-15", connect = db$con)

##the pitchrx package fails to get urls after 2017 due to an MLB formatting change. the mlbgameday code errors out if trying to scrape spring training
##games without pitchfx data (the rbind fails because the data has missing columns). 
##I am not good enough at R to know how to edit other people's packages although the change to pitchRx is literally a one character change.
##so I use mlbgameday's search_gids to get the urls, then scrape them with pitchrx.

ids_2018 = search_gids(start = "2018-01-01", end = "2018-12-31")

scrape(game.ids=ids_2018, connect = db$con)

#combine pitch and plate appearance data

pitch_data = dbGetQuery(db$con, 'select des,id,tfs,x,y,sv_id,start_speed,end_speed,sz_top,sz_bot,pfx_x,
                      pfx_z,break_angle,break_length,pitch_type,zone,spin_rate,url,num,event_num,count,gameday_link,inning_side,inning from pitch' )
ab_data = dbGetQuery(db$con, 'select pitcher,batter,pitcher_name,batter_name,num,start_tfs,stand,p_throws,event,gameday_link,date,event_num from atbat' )
unique(ab_data$event_num)
unique(ab_data$gameday_link)
unique(pitch_data$gameday_link)
head(ab_data)
head(pitch_data)
pitch_comb = sqldf("select a.*, b.pitcher,b.batter,b.pitcher_name,b.batter_name,b.start_tfs,b.stand,b.p_throws,b.event,b.date
                   from pitch_data a
                   left join ab_data b
                   on a.gameday_link = b.gameday_link
                   and a.num = b.num")
rm(ab_data)
rm(pitch_data)
pitch_comb = unique(pitch_comb)
#label seasons and spring training

pitch_comb$date = ymd(pitch_comb$date)
pitch_comb$season = year(pitch_comb$date)

#1 = spring, 2 = regular, 3 = post
pitch_comb$season_type = ifelse(pitch_comb$season == 2013 & pitch_comb$date < "2013-04-01",1,2)
pitch_comb$season_type = ifelse(pitch_comb$season == 2013 & pitch_comb$date > "2013-09-30",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2014 & pitch_comb$date < "2014-03-30",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2014 & pitch_comb$date > "2014-09-28",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2015 & pitch_comb$date < "2015-04-05",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2015 & pitch_comb$date > "2015-10-02",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2016 & pitch_comb$date < "2016-04-03",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2016 & pitch_comb$date > "2016-10-02",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2017 & pitch_comb$date < "2017-04-02",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2017 & pitch_comb$date > "2017-10-01",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2018 & pitch_comb$date < "2018-03-29",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2018 & pitch_comb$date > "2018-10-01",3,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2019 & pitch_comb$date < "2019-03-28",1,pitch_comb$season_type)
pitch_comb$season_type = ifelse(pitch_comb$season == 2019 & pitch_comb$date > "2019-09-29",3,pitch_comb$season_type)

#label if pitch is a fastball (all types). FF, CU, FT, FC, SI, FS are fastballs.

pitch_comb$FB = ifelse(pitch_comb$pitch_type %in% c("FF","FT","FC","SI","FS"),1,0)

#calculate woba using same scale as statcast (I made the list by hand)
#write.csv(unique(pitch_comb$event),"event_types_woba.csv")


woba_event_val = read.csv("woba_event_list.csv")

pitch_comb = sqldf("select a.*, b.woba_value from pitch_comb a
                   left join woba_event_val b
                   on a.event = b.Event_Type")

#find the pitch that closes out each at bat for calculation of PA purposes. maybe a better way to do this but I am dumb
terminal_event = sqldf("select gameday_link,pitcher,batter,num,max(id) as last_pitch_num from pitch_comb
                       group by gameday_link,pitcher,batter,num")

pitch_comb = sqldf("select a.*, b.last_pitch_num as terminal_event from pitch_comb a
                   left join terminal_event b
                   on a.gameday_link = b.gameday_link
                   and a.pitcher = b.pitcher
                   and a.batter = b.batter
                   and a.num = b.num")
#find whether the pitcher started the game (starter is the pitcher who pitched the first pa in the first half inning of each game)
first_pa_side = sqldf("select gameday_link,inning_side,min(num) as first_pa from pitch_comb group by gameday_link,inning_side")
pitch_comb = sqldf("select a.*, b.first_pa from pitch_comb a
                   left join first_pa_side b
                   on a.gameday_link = b.gameday_link
                   and a.inning_side = b.inning_side")
starter_list = sqldf("select distinct gameday_link,inning_side,pitcher from pitch_comb where first_pa = num")
#starters_game = sqldf("select gameday_link, count(*) as starters from starter_list group by gameday_link")
#subset(starters_game,starters>2) - should be empty and was
pitch_comb = sqldf("select a.*, case when b.pitcher is not null then 1 else 0 end as is_starter
                   from pitch_comb a
                   left join starter_list b
                   on a.gameday_link = b.gameday_link
                   and a.pitcher = b.pitcher")

#get leagues of each team, throw out non-mlb games
pitch_comb$league_t1 = substr(pitch_comb$gameday_link,19,21)
pitch_comb$league_t2 = substr(pitch_comb$gameday_link,26,28)

pitch_comb = subset(pitch_comb, league_t1=="mlb" & league_t2 =="mlb")

pitch_comb = subset(pitch_comb, select = -c(league_t1,league_t2))
pitch_comb$away_team = substr(pitch_comb$gameday_link,16,18)
pitch_comb$home_team = substr(pitch_comb$gameday_link,23,25)
pitch_comb$pitcher_team = ifelse(pitch_comb$inning_side=="top",pitch_comb$home_team,pitch_comb$away_team)

head(pitch_comb)
#done with the database. I like to write from a CSV here for safe keeping. first all pitches
#write.csv(pitch_comb,"pfx_comb.csv")
#then only those pitches that ended a PA
#write.csv(subset(pitch_comb,id==terminal_event),"pfx_comb_pa.csv")
#minimal data with no pitch info
pitch_comb_min = subset(pitch_comb,terminal_event == id & season_type == 2)
pitch_comb_min = subset(pitch_comb_min,select=c(pitcher_name,batter_name,gameday_link,inning_side,inning,stand,p_throws,event,date,season,away_team,home_team,pitcher_team))
write.csv(pitch_comb_min,"pa_data_min.csv")

# unique(pitch_comb$pitch_type)
# Pitcher_Game_Stats = sqldf("select pitcher,date,season,season_type,pitcher_team,home_team,is_starter,
#                            sum(case when FB=1 then start_speed else 0 end) as FB_V_T,
#                            sum(case when FB=1 then pfx_z else 0 end) as FB_Z_T,
#                            sum(case when FB=1 then pfx_x else 0 end) as FB_X,
#                            sum(case when FB=1 and zone < 10 then 1 else 0 end) as FB_Strikes,
#                            sum(case when FB=1 and start_speed is not null then 1 else 0 end) as FB_C,
#                            sum(case when FB=0 then start_speed else 0 end) as OS_V_T,
#                            sum(case when FB=0 then pfx_z else 0 end) as OS_Z_T,
#                            sum(case when FB=0 then pfx_x else 0 end) as OS_X,
#                            sum(case when FB=0 and zone < 10 then 1 else 0 end) as OS_Strikes,
#                            sum(case when FB=0 and start_speed is not null then 1 else 0 end) as OS_C,
#                            sum(case when pitch_type == 'SL' then 1 else 0 end) as SL_C, 
#                            sum(case when pitch_type == 'CU' or pitch_type == 'SC' or pitch_type == 'KC' then 1 else 0 end) as CU_C,
#                            sum(case when pitch_type == 'CH' then 1 else 0 end) as CH_C,
#                            sum(case when pitch_type == 'FF' then 1 else 0 end) as FF_C,
#                            sum(case when pitch_type == 'FT' or pitch_type == 'SI' or pitch_type == 'FS' then 1 else 0 end) as FS_C,
#                            sum(case when terminal_event = id then 1 else 0 end) as PA,
#                            sum(case when terminal_event = id and (event = 'Strikeout' or event = 'Strikeout - DP') then 1 else 0 end) as K,
#                            sum(case when terminal_event = id and (event = 'Walk') then 1 else 0 end) as BB,
#                            sum(case when terminal_event = id then woba_value else 0 end) as woba_value
#                            from pitch_comb group by pitcher,date,season,season_type,pitcher_team,home_team,is_starter
#                            ")
# head(Pitcher_Game_Stats)
# write.csv(Pitcher_Game_Stats,"Pitcher_Game_Stats_Pfx.csv")
#note that not all spring games actually have pitchfx data
