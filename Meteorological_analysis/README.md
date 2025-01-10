# Spark-based meteorological monitoring data analysis
## 1.Screen the top 20 periods with the highest concentration of six major pollutants
Start Spark Shell
```
:load <select_period.scala-path>
```
## 2.Concentration interval distribution
```
:load <cal_distribution.scala-path>
```
## 3.Calculate AQI index
```
:load <cal_AQI.scala-path>
```
## 4.Analyze pollutant relationships
- Analyze the relationship between SO2 concentration and NO2 concentration
- Analyze the relationship between PM10 concentration and air humidity
- Analyze the relationship between O3 concentration and cloud cover
## 5.Visualization
```
python3 visualization.py
```
![image name](https://github.com/AI-imp/Datamining/blob/main/Meteorological_analysis/visualization.png?raw=true)




