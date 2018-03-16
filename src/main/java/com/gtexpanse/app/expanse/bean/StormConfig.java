package com.gtexpanse.app.expanse.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * storm集群相关的配置
 */
@Data
public class StormConfig implements Serializable {

    private static final long serialVersionUID = 7061629735120681030L;

    private int spoutCount;
    private int boltCount;
    private int workerCount;
    private int maxPendingCount;
    private String topologyName;

}
