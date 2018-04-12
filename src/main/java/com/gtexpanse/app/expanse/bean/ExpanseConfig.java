package com.gtexpanse.app.expanse.bean;

import com.alibaba.fastjson.JSON;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Expanse config - 配置总成
 */
public class ExpanseConfig implements Serializable {

    private static final long serialVersionUID = -6047441989252839585L;

    /**
     * es集群信息 - 业务数据写入的es
     */
    private ESConfig es;

    /**
     * kafka 消费相关的配置
     */
    private KafkaConfig kafka;

    /**
     * storm 相关的配置
     */
    private StormConfig storm;

    /**
     * 增量支撑服务的rest地址 - 如保单增量支撑服务提供的保单数据捞取服务
     */
    private String restServiceUrl;

    /**
     * stormOutput的字段 - 尽量精简以减小网络流量
     */
    private List<String> transferFields;

    /**
     * 字段名 - 业务数据映射在 es index doc 的 id
     */
    private String indexMappingId;

    /**
     * es log 集群信息 - 保存消息处理记录供查询问题的es
     */
    private ESConfig esLog;

    /**
     * 消息处理handler
     */
    private String messageHandlerClass;

    /**
     * 从project目录/conf/下读取配置文件
     *
     * @param file file
     * @return config
     * @throws IOException exception
     */
    public static ExpanseConfig loadConfig(String file) throws IOException {
        File configFile = new File(System.getProperty("user.dir") + "/conf/" + file);
        if (!configFile.exists()) {
            throw new RuntimeException("storm-expanse configFile : [" + configFile + "] dose not exists!");
        }
        String fc = Files.toString(configFile, Charset.defaultCharset());
        return JSON.parseObject(fc, ExpanseConfig.class);
    }

    public ESConfig getEs() {
        return es;
    }

    public void setEs(ESConfig es) {
        this.es = es;
    }

    public KafkaConfig getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfig kafka) {
        this.kafka = kafka;
    }

    public StormConfig getStorm() {
        return storm;
    }

    public void setStorm(StormConfig storm) {
        this.storm = storm;
    }

    public String getRestServiceUrl() {
        return restServiceUrl;
    }

    public void setRestServiceUrl(String restServiceUrl) {
        this.restServiceUrl = restServiceUrl;
    }

    public List<String> getTransferFields() {
        return transferFields;
    }

    public void setTransferFields(List<String> transferFields) {
        this.transferFields = transferFields;
    }

    public String getIndexMappingId() {
        return indexMappingId;
    }

    public void setIndexMappingId(String indexMappingId) {
        this.indexMappingId = indexMappingId;
    }

    public ESConfig getEsLog() {
        return esLog;
    }

    public void setEsLog(ESConfig esLog) {
        this.esLog = esLog;
    }

    public String getMessageHandlerClass() {
        return messageHandlerClass;
    }

    public void setMessageHandlerClass(String messageHandlerClass) {
        this.messageHandlerClass = messageHandlerClass;
    }
}
