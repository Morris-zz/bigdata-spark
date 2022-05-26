package com.morris.spark.java.streaming;

import cn.hutool.core.date.DateUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;

/**
 * @author: zhaozheng
 * @date: 2022/5/25 2:37 下午
 * @description:
 */
public class StreamApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("ip").setMaster("local[3]").set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("127.0.0.1", 9999);
        JavaDStream<String> filter = dStream.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(word -> word.trim().length() > 0);


        JavaPairDStream<String, Integer> javaPairDStream = filter.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> reduceByKey = javaPairDStream.reduceByKey((item, tmp) -> item + tmp);
        reduceByKey.print(10);
    /*    reduceByKey.foreachRDD((rdd,time)->{
            String date = DateUtil.format(new Date(time.milliseconds()), "yyyyMMddHHmmss");
            if (!rdd.isEmpty()){
                rdd.coalesce(1).saveAsTextFile("/datas/zz/ip-"+date);
            }

        });*/
        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                String date = DateUtil.format(new Date(), "yyyyMMddHHmmss");
               stringIntegerJavaPairRDD.saveAsTextFile("/datas/zz/ip-"+date);
            }
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.stop(true,true);
    }
}
