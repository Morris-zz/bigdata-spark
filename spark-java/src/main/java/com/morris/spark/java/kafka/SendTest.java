package com.morris.spark.java.kafka;

import com.alibaba.fastjson.JSONObject;
import com.morris.spark.java.bean.SearchBean;

import java.io.IOException;
import java.util.Random;

/**
 * @author: zhaozheng
 * @date: 2022/5/25 3:53 下午
 * @description:
 */
public class SendTest {
    public static void main(String[] args) throws IOException {
        KafkaDmpProducer kafkaDmpProducer = new KafkaDmpProducer("dev");
        for (int i = 0; i < 100000; i++) {
            Random random = new Random();
            int nextInt = random.nextInt();
            SearchBean searchBean = new SearchBean();
            searchBean.setIp(String.valueOf(random.nextInt()));
            searchBean.setTopic("");
            searchBean.setKeyWord("word-"+ random.nextInt());
            String jsonString = JSONObject.toJSONString(searchBean);
            kafkaDmpProducer.sendMessage("morris-big-data",jsonString);
            System.out.println(jsonString);
        }

    }
}
