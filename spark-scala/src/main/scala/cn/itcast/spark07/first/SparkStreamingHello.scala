package cn.itcast.spark07.first

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Date

/**
 * @author: zhaozheng
 * @date: 2022/5/24 2:51 下午
 * @description:
 */
object SparkStreamingHello {
  def main(args: Array[String]): Unit = {
    // TODO: 1. 创建StreamingContext流式上下文对象，传递时间间隔
    val ssc: StreamingContext = {
      // a. 创建SparkConf对象，设置应用属性，比如名称和master
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]") // 启动3个Thread线程
      // b. 设置批处理时间间隔BatchInterval：5秒
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      new StreamingContext(sparkConf, Seconds(60))
    }
    val input: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999)
  /*  val resDStream: DStream[(String, Int)] = input
      .filter(line => line != null && line.trim.nonEmpty)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((v1, v2) => v1 + v2)*/

    val resDStream: DStream[(String, Int)] = input.transform(rdd => {
      rdd.filter(line => line != null && line.trim.nonEmpty)
        .flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey((v1, v2) => v1 + v2)
    })
    resDStream.print(10)
    resDStream.foreachRDD((rdd,time)=>{
      //转换time
      val dt: String = FastDateFormat
        .getInstance("yyyyMMddHHmmss").format(new Date(time.milliseconds))
      println(s"###time====>$dt")
      if (!rdd.isEmpty()){
        rdd.coalesce(1)
        rdd.foreachPartition(rdd=>rdd.foreach(println));
        rdd.saveAsTextFile(s"datas/zz/wc-output-${dt}")
      }
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }

}
