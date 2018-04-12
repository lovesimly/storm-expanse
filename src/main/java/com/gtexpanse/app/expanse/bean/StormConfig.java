package com.gtexpanse.app.expanse.bean;

import java.io.Serializable;

/**
 * storm集群相关的配置
 */
public class StormConfig implements Serializable {

    private static final long serialVersionUID = 7061629735120681030L;

    private int spoutCount;
    private int boltCount;
    private int workerCount;
    private int maxPendingCount;
    private String topologyName;

    public int getSpoutCount() {
        return spoutCount;
    }

    public void setSpoutCount(int spoutCount) {
        this.spoutCount = spoutCount;
    }

    public int getBoltCount() {
        return boltCount;
    }

    public void setBoltCount(int boltCount) {
        this.boltCount = boltCount;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getMaxPendingCount() {
        return maxPendingCount;
    }

    public void setMaxPendingCount(int maxPendingCount) {
        this.maxPendingCount = maxPendingCount;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }
}
