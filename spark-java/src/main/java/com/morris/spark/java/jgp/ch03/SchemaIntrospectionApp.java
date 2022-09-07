package com.morris.spark.java.jgp.ch03;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * @author: zhaozheng
 * @date: 2022/8/8 3:19 下午
 * @description:
 */
public class SchemaIntrospectionApp {
    public static void main(String[] args) throws AnalysisException {
        SchemaIntrospectionApp schemaIntrospectionApp = new SchemaIntrospectionApp();
        schemaIntrospectionApp.start(args);
    }

    private void start(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder()
                .appName("schema")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("data/Restaurants_in_Wake_County_NC.csv");
        df.createTempView("temp");
        spark.sql("select * from temp").show(10);
        Dataset<Row> dataset = spark.sql("select count(1) as cnt from temp");
        dataset.show(10);
       /* JavaRDD<Row> rowJavaRDD = dataset.toJavaRDD();
        rowJavaRDD.foreach(e->{
            long aLong = e.getLong(0);
            System.out.println("####"+aLong);
        });*/

        Row first = dataset.first();
        Long cnt = first.getAs("cnt");
        System.out.println("#####"+cnt);

        /*df = df.withColumn("country",lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY");
        df = df.withColumn("id",concat(
                df.col("state"),
                lit("_"),
                df.col("country"),lit("_"),
                df.col("datasetId")

        ));
        StructType schema = df.schema();
        schema.printTreeString();
        String schemaAsString = schema.mkString();
        System.out.println("*** Schema as string: " + schemaAsString);
        String schemaAsJson = schema.prettyJson();
        System.out.println("*** Schema as JSON: " + schemaAsJson);
*/
    }
}
