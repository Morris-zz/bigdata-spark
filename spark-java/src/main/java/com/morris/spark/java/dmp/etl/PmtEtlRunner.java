package com.morris.spark.java.dmp.etl;

import com.morris.spark.java.dmp.config.Constants;
import com.morris.spark.java.dmp.config.PropertiesUtil;
import com.morris.spark.java.dmp.util.DateUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author: zhaozheng
 * @date: 2022/5/5 7:36 下午
 * @description:
 */
public class PmtEtlRunner {
    final static Properties props = PropertiesUtil.getProperties();

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName(PmtEtlRunner.class.getName())
                .master("local[2]")
                .getOrCreate();
        //加载数据

        Dataset<Row> pmtDs = sparkSession.read().json(props.getProperty(Constants.DATAS_PATH));
        processData(pmtDs);


    }

    /**
     * 解析IP地址为省份和城市
     *
     * @param pmtDs
     */
    private static void processData(Dataset<Row> pmtDs) {
        SparkSession sparkSession = pmtDs.sparkSession();
        //加载字典
        sparkSession.sparkContext().addFile(props.getProperty(Constants.IPS_DATA_REGION_PATH));
        JavaRDD<Tuple3> ip1 = pmtDs.javaRDD().mapPartitions(iter -> {
            List<Tuple3> rows = new ArrayList<>();
            try {
                DbSearcher dbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"));
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String ip = row.getAs("ip");
                    DataBlock dataBlock = dbSearcher.btreeSearch(ip);
                    String region = dataBlock.getRegion();
                    int cityId = dataBlock.getCityId();
                    rows.add(new Tuple3(region, cityId, DateUtils.getTodayDate2()));
                }
                return rows.iterator();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return rows.iterator();
        });
        ip1.foreach(e-> System.out.println(e.toString()));


    }
}
