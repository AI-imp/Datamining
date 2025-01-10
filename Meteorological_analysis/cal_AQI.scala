// 定义 AQI 计算函数
def calcAQI(pm: Double, pollutant: String): Double = {
  val c = pm // 监测浓度
  val bp = Array(0, 35, 75, 115, 150, 250, 350, 500) // 污染物浓度分界值
  val iaqi = Array(0, 50, 100, 150, 200, 300, 400, 500) // 污染物空气质量分指数分界值

  val idx = bp.indexWhere(_ > c) match {
    case 0 => 0
    case -1 => 7
    case i => i
  }

  val ihi = iaqi(idx)
  val ilo = if (idx == 0) 0 else iaqi(idx - 1)
  val chi = bp(idx)
  val clo = if (idx == 0) 0 else bp(idx - 1)

  // 计算并返回 AQI
  Math.round((ihi - ilo) * (c - clo) / (chi - clo) + ilo)
}

// 定义计算 AQI、首要污染物和空气质量等级的函数
def calcAirQuality(values: Seq[Double]): (Double, String, String) = {
  val pollutant = Seq("SO2", "NO2", "PM10", "PM2.5", "O3", "CO")

  // 计算各污染物的 AQI
  val aqi = pollutant.map(p => calcAQI(values(pollutant.indexOf(p)), p))

  // 计算最大 AQI 和首要污染物
  val primaryPollutant = pollutant(aqi.indexOf(aqi.max))

  // 确定空气质量等级
  val level = aqi.max match {
    case a if a <= 50 => "优"
    case a if a <= 100 => "良"
    case a if a <= 150 => "轻度污染"
    case a if a <= 200 => "中度污染"
    case a if a <= 300 => "重度污染"
    case _ => "严重污染"
  }

  // 返回 AQI、首要污染物和空气质量等级
  (aqi.max, primaryPollutant, level)
}

// 读取数据并转换数据类型
val data2 = spark.read.format("csv").option("header", "true").load("/home/dolphin/Desktop/data2.csv")
val columnsToConvert = Seq("SO2", "NO2", "PM10", "PM25", "O3", "CO")
val data2Double = columnsToConvert.foldLeft(data2) { (data, column) =>
  data.withColumn(column, col(column).cast(DoubleType))
}

// 选择需要的列
val selected = data2Double.select("date", "SO2", "NO2", "PM10", "PM25", "O3", "CO")

// 转换数据为数组形式
val dataArray = selected.collect().map(row =>
  Array(row.getAs[String]("date"), row.getAs[Double]("SO2"), row.getAs[Double]("NO2"),
        row.getAs[Double]("PM10"), row.getAs[Double]("PM25"), row.getAs[Double]("O3"),
        row.getAs[Double]("CO"))
)

// 打开CSV文件，写入表头
import java.io.{File, PrintWriter}
val csv = new PrintWriter(new File("/home/dolphin/Desktop/output2.csv"))
csv.println("日期,AQI,首要污染物,空气质量等级") // 写入表头

// 写入数据并计算 AQI
for (values <- dataArray) {
  val date = values(0) // 日期
  val pollutantValues = values.drop(1) // 除日期外的污染物浓度值
  val (aqi, primaryPollutant, level) = calcAirQuality(pollutantValues)

  // 写入计算结果到 CSV
  val line = s"${date},${aqi},${primaryPollutant},${level}"
  csv.println(line)
}

// 关闭 CSV 文件
csv.close()
