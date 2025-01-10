// 导入 Spark 相关库
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

// 创建 SparkSession 对象
val spark = SparkSession.builder()
  .appName("Read CSV File and Create Temp View")
  .master("local")
  .getOrCreate()

// 读取 CSV 文件并创建临时视图
val df: DataFrame = spark.read.format("csv")
  .option("header", "true")
  .load("file:///home/dolphin/Desktop/res.csv")

// 创建名为 res 的临时视图
df.createOrReplaceTempView("res")

// 选取 `PM2.5监测浓度(μg/m3)` 字段，并统计不同浓度区间的数量
val PM25_total = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` >= 0").count()

val PM25_1 = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` >= 0 AND `PM2.5监测浓度(μg/m3)` <= 35").count()

val PM25_2 = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` > 35 AND `PM2.5监测浓度(μg/m3)` <= 75").count()

val PM25_3 = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` > 75 AND `PM2.5监测浓度(μg/m3)` <= 115").count()

val PM25_4 = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` > 115 AND `PM2.5监测浓度(μg/m3)` <= 150").count()

val PM25_5 = spark.sql("SELECT `PM2.5监测浓度(μg/m3)` FROM res WHERE `PM2.5监测浓度(μg/m3)` > 150 AND `PM2.5监测浓度(μg/m3)` <= 250").count()

// 创建结果数据集
import org.apache.spark.sql.Row

val intervalData = Seq(
  Row("总量", PM25_total),
  Row("第一区间:0-35", PM25_1),
  Row("第二区间:35-75", PM25_2),
  Row("第三区间:75-115", PM25_3),
  Row("第四区间:115-150", PM25_4),
  Row("第五区间:150-250", PM25_5)
)

// 定义 Schema
val schema = StructType(Seq(
  StructField("区间等级", StringType),
  StructField("数量", LongType)
))

// 创建 DataFrame
val resultDF = spark.createDataFrame(spark.sparkContext.parallelize(intervalData), schema)

// 显示结果
resultDF.show()
