package com.gtexpanse.app.expanse.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * elasticsearch 相关配置
 */
@Data
public class ESConfig implements Serializable {

    private static final long serialVersionUID = -777631073696526405L;

    private String index;
    private String type;
    private String clusterName;
    private String clusterUrls;

}
