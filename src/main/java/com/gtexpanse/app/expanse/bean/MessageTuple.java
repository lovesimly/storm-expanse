package com.gtexpanse.app.expanse.bean;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Expanse - message 的包装类
 */
public class MessageTuple implements Serializable {

    private static final long serialVersionUID = 5479053979599591518L;

    private final String msgId; //kafka msg key
    private final Map<String, Object> message; //decode之后的消息体,只含有bolt所需的必要字段
    private final Integer dbActionFlag; //INSERT(0), DELETE(1), UPDATE(2)
    private final long createMs;
    private final AtomicInteger failureTimes;
    private long emitMs;

    private transient boolean isSuccess;
    private transient CountDownLatch latch;

    public MessageTuple(String id, int dbActionFlag, Map<String, Object> map) {
        this.msgId = id;
        this.dbActionFlag = dbActionFlag;
        this.message = map;

        this.failureTimes = new AtomicInteger(0);
        this.createMs = System.currentTimeMillis();
        // 初始count为1，ack或者fail时-1
        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    /**
     * Tuple开始执行，latch await for 4 hours
     *
     * @return await
     * @throws InterruptedException e
     */
    public boolean waitForHandle() throws InterruptedException {
        return latch.await(4, TimeUnit.HOURS);
    }

    public void updateEmitMs() {
        emitMs = System.currentTimeMillis();
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public String toString() {
        return "MessageTuple{" + "msgId='" + msgId + '\'' + ", message=" + JSON.toJSONString(message)
                + ", dbActionFlag=" + DBMSAction.getShotName(dbActionFlag) + ", failureTimes=" + failureTimes + ", isSuccess=" + isSuccess
                + ", createMs=" + createMs + ", emitMs=" + emitMs + '}';
    }

    public String getMsgId() {
        return msgId;
    }

    public int getDbActionFlag() {
        return dbActionFlag;
    }

    public Map<String, Object> getMessage() {
        return message;
    }

    public AtomicInteger getFailureTimes() {
        return failureTimes;
    }
}
