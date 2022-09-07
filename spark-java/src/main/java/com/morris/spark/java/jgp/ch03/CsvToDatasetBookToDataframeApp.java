package com.morris.spark.java.jgp.ch03;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * @author: zhaozheng
 * @date: 2022/8/8 3:19 下午
 * @description:
 */
public class CsvToDatasetBookToDataframeApp {
    public static void main(String[] args) {
        CsvToDatasetBookToDataframeApp csvToDatasetBookToDataframeApp = new CsvToDatasetBookToDataframeApp();
        csvToDatasetBookToDataframeApp.start(args);
    }
    public void start(String[] args){
        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<Book> and back")
                .master("local")
                .getOrCreate();
        spark.read().format("csv");
    }
}
