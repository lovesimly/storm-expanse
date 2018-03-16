package com.gtexpanse.app.expanse.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * kafka 配置
 */
@Data
public class KafkaConfig implements Serializable {

    private static final long serialVersionUID = -2287359397900022138L;
    private String topic;
    private String groupId;
    private String brokers;

}
