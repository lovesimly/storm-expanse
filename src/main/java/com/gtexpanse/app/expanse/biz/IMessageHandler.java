package com.gtexpanse.app.expanse.biz;

import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.bean.MessageTuple;

public interface IMessageHandler {

    /**
     * 初始化 message handler
     */
    void init(ExpanseConfig conf);

    /**
     * 处理消息
     *
     * @param tuple tuple
     * @return 错误消息str or "",null
     * @throws Exception exception
     */
    String execute(MessageTuple tuple) throws Exception;

    /**
     * 释放资源
     */
    void cleanup();

}
