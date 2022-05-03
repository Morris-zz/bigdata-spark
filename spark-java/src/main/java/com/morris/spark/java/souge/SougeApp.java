package com.morris.spark.java.souge;

import cn.hutool.json.JSONUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SougeApp {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("zz");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sc.textFile("/Users/zz/Documents/01-project/bigdata-spark/datas/souge/SogouQ.sample",2);
        //1 词热搜
        JavaPairRDD<String, Integer> pairRDD = rdd.mapPartitionsToPair(iter -> {
            ArrayList<Tuple2<String, Integer>> words = new ArrayList<>();
            while (iter.hasNext()) {
                String line = iter.next();
                if (StringUtils.isNotEmpty(line) && line.trim().split("\\s+").length == 6) {
                    String word = line.split("\\s+")[2].replace("[", "").replace("]", "");
                    Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>(word, 1);
                    words.add(tuple2);
                }
            }
            return words.iterator();
        }).reduceByKey(Integer::sum);
        pairRDD.mapToPair(kv->new Tuple2<Integer,String>(kv._2,kv._1))
                .sortByKey(false)
                .take(100)
                .forEach(t-> System.out.println(t._1+" : "+t._2));
        //2 用户搜索词
        JavaPairRDD<Tuple2<String, String>, Integer> pairRDD1 = rdd.mapPartitionsToPair(iter -> {
            ArrayList<Tuple2<Tuple2<String, String>, Integer>> words = new ArrayList<>();
            while (iter.hasNext()) {
                String line = iter.next();
                if (StringUtils.isNotEmpty(line) && line.trim().split("\\s+").length == 6) {
                    String word = line.split("\\s+")[2].replace("[", "").replace("]", "");
                    Tuple2<Tuple2<String, String>, Integer> tuple2 = new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(line.split("\\s+")[1], word), 1);
                    words.add(tuple2);
                }
            }
            return words.iterator();
        });
//        pairRDD1.reduceByKey((tmp,item)->tmp+item).t
    }
}
