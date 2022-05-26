package com.morris.spark.java.kafka;

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
            kafkaDmpProducer.sendMessage("morris-big-data","test-"+nextInt);
            System.out.println("test-"+nextInt);
        }

    }
}
