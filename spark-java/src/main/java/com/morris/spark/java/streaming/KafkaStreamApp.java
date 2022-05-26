package com.morris.spark.java.streaming;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import com.morris.spark.java.bean.SearchBean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import scala.Tuple2;

import java.util.*;

/**
 * @author: zhaozheng
 * @date: 2022/5/25 2:37 下午
 * @description:
 */
public class KafkaStreamApp {
    public static void main(String[] args) throws InterruptedException {
        //jssc
        SparkConf sparkConf = new SparkConf().setAppName("appp").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.122.7.5:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("morris-big-data");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        JavaPairDStream<String, String> mapToPair = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        JavaPairDStream<String, ArrayList<SearchBean>> stringArrayListJavaPairDStream = mapToPair.mapPartitionsToPair(partition -> {
            ArrayList<Tuple2<String, ArrayList<SearchBean>>> tuple2s = new ArrayList<>();
            Random random = new Random();

            while (partition.hasNext()) {
                Tuple2<String, String> item = partition.next();
                ArrayList<SearchBean> searchBeans = new ArrayList<>();
                String topicName = item._1;
                String json = item._2;
                SearchBean searchBean = JSONObject.parseObject(json, SearchBean.class);
                searchBeans.add(searchBean);
                tuple2s.add(new Tuple2<String, ArrayList<SearchBean>>(topicName + "@" + random.nextInt(), searchBeans));
            }
            return tuple2s.iterator();

        }).reduceByKey((tmp, item) -> {
            tmp.addAll(item);
            return tmp;
        });
        stringArrayListJavaPairDStream.print(10);


        jssc.start();
        jssc.awaitTermination();
        jssc.stop(true,true);
    }
}
