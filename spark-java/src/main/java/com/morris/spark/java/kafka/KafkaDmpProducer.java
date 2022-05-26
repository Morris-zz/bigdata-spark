package com.morris.spark.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * @author: zhaozheng
 * @date: 2021/11/11 11:49 上午
 * @description:
 */

public class KafkaDmpProducer implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(KafkaDmpProducer.class);

    public static final int PARTITION_NUM = 14;
    private KafkaProducer producer;

    public KafkaDmpProducer(String evn) throws IOException {
        String kafkaServer = KafkaConstants.BOOTSTRAP_SERVERS_NEW_DEV;
        /*switch (evn){
            case "test":kafkaServer=KafkaConstants.BOOTSTRAP_SERVERS_NEW;
            break;
            case "us":kafkaServer=KafkaConstants.BOOTSTRAP_SERVERS_NEW_PROD_US;
                break;
            case "eu":kafkaServer=KafkaConstants.BOOTSTRAP_SERVERS_NEW_PROD_EU;
                break;
            default: break;
        }*/

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);//xxx服务器ip
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        props.put("linger.ms", 100);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);

    }



    /**
     * 异步发送，不保证可靠性
     *
     * @param msg
     * @param memberId
     */
    public void sendMessage(String topic,String msg, String memberId) {
        producer.send(new ProducerRecord(topic, memberId,msg));
//        producer.send(new ProducerRecord(topic, null, null, memberId, msg, null));
    }


    /**
     * 异步发送，不保证可靠性
     *
     * @param msg
     */
    public void sendMessage(String topic,String msg) {
        producer.send(new ProducerRecord(topic,msg));
//        producer.send(new ProducerRecord(topic, null, null, memberId, msg, null));
    }


    /**
     * 异步发送，不保证可靠性
     *
     * @param
     */
    public void sendMessage(ProducerRecord producerRecord) {
        producer.send(producerRecord);
//        producer.send(new ProducerRecord(topic, null, null, memberId, msg, null));
    }






}
