import org.apache.spark.sql.functions.col

// 读取数据
val df: DataFrame = spark.read.format("csv").option("header", "true").load("file:///home/dolphin/Desktop/res.csv")

// 计算 SO2 和 NO2 的关系
val df1 = spark.sql("select `监测时间`, `SO2监测浓度(μg/m3)`, `NO2监测浓度(μg/m3)` from SO2 where `SO2监测浓度(μg/m3)` > 0")

// 写入 CSV
df1.write.option("header", true).mode("overwrite").csv("file:///home/dolphin/Desktop/SO2_NO2.csv")

// 数据选择和描述统计
val df_SO2_NO2 = df1.select(col("`SO2监测浓度(μg/m3)`").as("SO2"),
                             col("`NO2监测浓度(μg/m3)`").as("NO2"))
val df_desc = df_SO2_NO2.describe()
df_desc.show()
