# Taobao user behavior data mining based on Hive
## 1.Preparation
- install MySQL、Sqoop、Hadoop、Hive
- Import data from MySQL to HDFS using Sqoop
## 2.Data_process
- Load data
```
bash data_load.sh <DB_HOST> <DB_USER> <DB_PASS> <DB_NAME>
```
- Data cleaning
```
bash data_cleaning.sh <DB_HOST> <DB_USER> <DB_PASS> <DB_NAME> <START_DATE> <END_DATE>
```
- Create tables based on data items required for mining
```
#用户总访问量PV，总用户量UV
create table zongfangwen as
select sum(case when behavior_type = 'pv' then 1
else 0 end) as pv,     
count(distinct user_id) as uv
from user_behavior;
```
```
#点击/(加购物车+收藏)/购买 , 各环节转化率
create table zhuanhualv as
select a.pv,
       a.fav,
       a.cart,
       a.fav + a.cart as "fav+cart",
       a.buy,
       round((a.fav + a.cart) / a.pv, 4) as pv2favcart,
       round(a.buy / (a.fav + a.cart), 4) as favcart2buy,
       round(a.buy / a.pv, 4) as pv2buy
from (
    select sum(pv) as pv,
           sum(fav) as fav,
           sum(cart) as cart,
           sum(buy) as buy
    from user_behavior_count
) as a;
```
```
#每个用户的购物情况，加工到 user_behavior_count
create table user_behavior_count as
select user_id,
       sum(case
when behavior_type = 'pv' then 1 else 0 end) as pv,
sum(case when behavior_type = 'fav' then 1 else 0 end) as fav,      
sum(case when behavior_type = 'cart' then 1 else 0 end) as cart,      
sum(case when behavior_type = 'buy' then 1 else 0 end) as buy 
from user_behavior
group by user_id;
#用户行为习惯
create table hour_behavior as
select
    hour(datetime) as hour,
    sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
    sum(case when behavior_type = 'fav' then 1 else 0 end) as fav,
    sum(case when behavior_type = 'cart' then 1 else 0 end) as cart,
    sum(case when behavior_type = 'buy' then 1 else 0 end) as buy
from
    user_behavior
group by
    hour(datetime)
order by
    hour;
```
```
#周用户的活跃分布
create table week as
select
    pmod(datediff(to_date(datetime), '1920-01-01') - 3, 7) as weekday,
    sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
    sum(case when behavior_type = 'fav' then 1 else 0 end) as fav,
    sum(case when behavior_type = 'cart' then 1 else 0 end) as cart,
    sum(case when behavior_type = 'buy' then 1 else 0 end) as buy
from
    user_behavior
where
    date(to_date(datetime)) between '2017-11-27' and '2017-12-03'
group by
    pmod(datediff(to_date(datetime), '1920-01-01') - 3, 7)
order by
    weekday;
```
## 3.Eclipse connects to Hive database
- Enter the etc/hadoop directory, edit the core-site.xml file, and configure the proxy user
- Enter the /conf directory and edit the hdfs-site.xml file
- After starting hadoop, enable metastore
```
hive --service metastore &
```
- Then start hiveserver2 and log in using beeline
```
hive --service hiveserver2 &
```
## 4.Visualization
- backend code：[dbtaobao.java](https://github.com/AI-imp/Datamining/blob/main/dbtaobao.java)
- Use echart for the front end
![img1](https://github.com/AI-imp/Datamining/blob/main/visualize/4.png?raw=true)
![img2](https://github.com/AI-imp/Datamining/blob/main/visualize/3.png?raw=true)
![img3](https://github.com/AI-imp/Datamining/blob/main/visualize/2.png?raw=true)
![img4](https://github.com/AI-imp/Datamining/blob/main/visualize/1.png?raw=true)






