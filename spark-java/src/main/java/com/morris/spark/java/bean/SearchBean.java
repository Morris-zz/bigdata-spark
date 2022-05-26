package com.morris.spark.java.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @author: zhaozheng
 * @date: 2022/5/25 4:22 下午
 * @description:
 */
@Data
public class SearchBean implements Serializable {
    private String topic;
    private String ip;
    private String keyWord;

}
