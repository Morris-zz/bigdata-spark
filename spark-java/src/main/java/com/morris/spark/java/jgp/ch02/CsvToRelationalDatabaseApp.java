package com.morris.spark.java.jgp.ch02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * @author: zhaozheng
 * @date: 2022/7/28 7:50 下午
 * @description:
 */
public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }
    private void start(){
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");
        df = df.withColumn("name"
                ,concat(df.col("lname"),lit(","),df.col("fname")));
        df.show(10);
        df.printSchema();
//        String url = "jdbc:mysql://**/tsp_user_server?autoReconnect=true&readOnlyPropagatesToServer=false&characterEncoding=utf8&useSSL=false";
//        Properties properties = new Properties();
//        properties.setProperty("username","**");
//        properties.setProperty("password","**");
//        properties.setProperty("driver-class-name","com.mysql.jdbc.Driver");
//        df.write()
//                .mode(SaveMode.Overwrite)
//                .jdbc(url,"ch02",properties);
    }
}
