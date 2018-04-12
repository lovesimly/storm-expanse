package com.gtexpanse.app.expanse.bean;

import java.io.Serializable;

/**
 * kafka 配置
 */
public class KafkaConfig implements Serializable {

    private static final long serialVersionUID = -2287359397900022138L;
    private String topic;
    private String groupId;
    private String brokers;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }
}
