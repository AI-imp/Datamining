val df: DataFrame = spark.read.format("csv").option("header", "true").load("file:///home/dolphin/Desktop/res.csv")

// 注册临时表
df.createOrReplaceTempView("temp_table")

// 筛选数据
val df1 = spark.sql("select `监测时间`, `O3监测浓度(μg/m3)`, `云量` from temp_table where `O3监测浓度(μg/m3)` > 0")

// 写入 CSV
df1.write.option("header", true).mode("overwrite").csv("file:///home/dolphin/Desktop/O3_cloud.csv")

// 数据转换与描述统计
val df_O3_cloud = df1.select(col("`O3监测浓度(μg/m3)`").cast("double").as("O3"),
                             col("`云量`").cast("double").as("cloud"))
val df_desc = df_O3_cloud.describe()
df_desc.show()
