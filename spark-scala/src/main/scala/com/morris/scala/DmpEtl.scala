package com.morris.scala

import com.morris.scala.config.ApplicationConfig
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{current_date, date_sub}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * @author: zhaozheng
 * @date: 2022/5/6 10:46 上午
 * @description:
 */
object DmpEtl {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(DmpEtl.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val dmpDF: DataFrame = sparkSession.read.json(ApplicationConfig.DATAS_PATH)
    //根据ip获取地理位置
    //加载字典
    sparkSession.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    val ipRDD: RDD[Row] = dmpDF.rdd.mapPartitions { iter =>
      //创建DS字典转换
      val searcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
      iter.map { row =>
        val ip = row.getAs[String]("ip")
        val dataBlock = searcher.btreeSearch(ip)
        val seq: Seq[Any] = row.toSeq :+ dataBlock.getRegion :+ dataBlock.getCityId
        Row.fromSeq(seq)
      }
    }
    println(ipRDD.take(1).mkString("Array(", ", ", ")"))
    val newSchema: StructType = dmpDF.schema
      .add(StructField("province", StringType, nullable = false))
      .add(StructField("city_id", IntegerType, nullable = false))
    val dwDF: DataFrame = sparkSession.createDataFrame(ipRDD, newSchema)
    dwDF.show(4,truncate = false)

    dwDF.withColumn("dt",date_sub(current_date(), 1).cast(StringType))
    dwDF.show(2,truncate = false)

  }

}
