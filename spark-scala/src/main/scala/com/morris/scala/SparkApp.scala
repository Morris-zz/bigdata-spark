package com.morris.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {
  def main(args: Array[String]): Unit = {
    val sc :SparkContext ={
      val conf = new SparkConf().setAppName("morris").setMaster("local[2]")
      new SparkContext(conf)
    }
    val rdd = sc.textFile("datas/souge/SogouQ.sample", 2)
   rdd.mapPartitions(line => {
      line.map(line => (line.split("\\s+")(2), 1))
    }).reduceByKey(_+_).sortBy( _._2,ascending = false).take(4).foreach(println)
  }
}
