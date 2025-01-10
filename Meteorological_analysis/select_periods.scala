import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

// 创建 SparkConf 对象
val conf = new SparkConf().setAppName("Air Pollution Analysis").setMaster("local")

// 创建 SparkContext 对象
val sc = new SparkContext(conf)

// 创建 SparkSession 对象
val spark = SparkSession.builder().config(conf).getOrCreate()

// 读取 CSV 文件
val df: DataFrame = spark.read.format("csv").option("header", "true").load("file:///home/Desktop/res.csv")

// 定义一个通用函数，用于筛选并保存前20名污染物数据
def processPollutant(pollutantName: String, pollutantColumn: String, outputPath: String): Unit = {
  // 创建临时视图
  df.createOrReplaceTempView(pollutantName)
  
  // SQL 查询前20的浓度
  val query = s"""
    SELECT `监测时间`, `$pollutantColumn`
    FROM $pollutantName
    WHERE `$pollutantColumn` > 0
    ORDER BY CAST(`$pollutantColumn` AS DOUBLE) DESC
    LIMIT 20
  """
  
  val result = spark.sql(query).orderBy("监测时间")
  
  // 写入 CSV 文件
  result.write.option("header", true).mode("overwrite").csv(outputPath)
}

// 处理六大污染物
processPollutant("SO2", "SO2监测浓度(μg/m3)", "file:///home/Desktop/SO2_20.csv")
processPollutant("NO2", "NO2监测浓度(μg/m3)", "file:///home/Desktop/NO2_20.csv")
processPollutant("PM10", "PM10监测浓度(μg/m3)", "file:///home/Desktop/PM10_20.csv")
processPollutant("PM25", "PM2.5监测浓度(μg/m3)", "file:///home/Desktop/PM25_20.csv")
processPollutant("O3", "O3监测浓度(μg/m3)", "file:///home/Desktop/O3_20.csv")
processPollutant("CO", "CO监测浓度(mg/m3)", "file:///home/Desktop/CO_20.csv")
