import org.apache.spark.sql.functions.{col, describe}

// 读取数据
val df: DataFrame = spark.read.format("csv").option("header", "true").load("file:///home/dolphin/Desktop/res.csv")

// 注册临时表
df.createOrReplaceTempView("temp_table")

// 筛选数据
val df1 = spark.sql("select `监测时间`, `PM10监测浓度(μg/m3)`, `湿度(%)` from temp_table where `PM10监测浓度(μg/m3)` > 0")

// 写入 CSV
df1.write.option("header", true).mode("overwrite").csv("file:///home/dolphin/Desktop/PM10_air.csv")

// 数据转换与描述统计
val df_PM10_air = df1.select(col("`PM10监测浓度(μg/m3)`").cast("double").as("PM10"),
                             col("`湿度(%)`").cast("double").as("air"))
val df_desc = df_PM10_air.describe()
df_desc.show()
