package com.gtexpanse.app.expanse.bean;

import java.io.Serializable;

/**
 * elasticsearch 相关配置
 */
public class ESConfig implements Serializable {

    private static final long serialVersionUID = -777631073696526405L;

    private String index;
    private String type;
    private String clusterName;
    private String clusterUrls;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterUrls() {
        return clusterUrls;
    }

    public void setClusterUrls(String clusterUrls) {
        this.clusterUrls = clusterUrls;
    }
}
