package com.gtexpanse.app.expanse.biz.impl;

import com.gtexpanse.app.expanse.bean.DBMSAction;
import com.gtexpanse.app.expanse.bean.MessageTuple;
import com.gtexpanse.app.expanse.biz.IMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * abstract MessageHandler
 */
public abstract class BaseHandler implements IMessageHandler {

    /**
     * 默认连接保持3秒
     */
    private static final int HTTP_TIMEOUT = 3000;

    protected final Logger log = LoggerFactory.getLogger(BaseHandler.class);

    RestTemplate restTemplate = init();

    @Override
    public String execute(MessageTuple messageTuple) throws Exception {
        if (DBMSAction.INSERT.getValue().equals(messageTuple.getDbActionFlag())) {
            return insert(messageTuple);
        }
        if (DBMSAction.UPDATE.getValue().equals(messageTuple.getDbActionFlag())) {
            return update(messageTuple);
        }
        if (DBMSAction.DELETE.getValue().equals(messageTuple.getDbActionFlag())) {
            return delete(messageTuple);
        }
        return null;
    }

    public abstract String insert(MessageTuple messageTuple) throws Exception;

    public abstract String update(MessageTuple messageTuple) throws Exception;

    public abstract String delete(MessageTuple messageTuple) throws Exception;

    private RestTemplate init() {
        HttpComponentsClientHttpRequestFactory httpClient = new HttpComponentsClientHttpRequestFactory();
        httpClient.setConnectTimeout(HTTP_TIMEOUT);
        return new RestTemplate(httpClient);
    }

}
