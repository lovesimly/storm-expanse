package com.gtexpanse.app.expanse.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.bean.MessageTuple;
import com.gtexpanse.app.expanse.biz.IMessageHandler;
import com.gtexpanse.app.expanse.utils.Constants;
import com.gtexpanse.app.expanse.utils.TpsCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class KafkaBolt implements IRichBolt, Serializable {

    private static final long serialVersionUID = -3378596634827075033L;

    /**
     * 独立日志输出
     */
    private static final Logger log = LoggerFactory.getLogger("EXPANSE-BOLT");

    private OutputCollector collector;

    private ExpanseConfig conf;

    private IMessageHandler messageHandler;

    private TpsCounter tpsCounter;

    public KafkaBolt(ExpanseConfig conf) {
        super();
        this.conf = conf;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        if (conf.getMessageHandlerClass() != null) {
            try {
                Class c = Class.forName(conf.getMessageHandlerClass());
                messageHandler = (IMessageHandler) c.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("IMessageHandler reflect occur error", e);
            }
        } else {
            throw new IllegalArgumentException("config.messageHandlerClass must not be null !");
        }

        messageHandler.init(conf);

        if (Constants.IS_COUNT_TPS) {
            tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());
        }
    }

    @Override
    public void execute(Tuple input) {
        if (Constants.IS_COUNT_TPS) {
            tpsCounter.count();
        }

        MessageTuple messageTuple = (MessageTuple) input.getValueByField("MessageTuple");
        log.info("bolt receive : " + messageTuple.getMsgId());
        try {
            String result = messageHandler.execute(messageTuple);
            if (result == null || Objects.equals(result, "")) {
                collector.ack(input);
            } else {
                log.error("message tuple " + messageTuple + " handle occur error : " + result);
                collector.fail(input);
            }
        } catch (Throwable e) {
            log.error("bolt execute tuple:" + messageTuple + " occur error:", e);
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        if (messageHandler != null) {
            messageHandler.cleanup();
        }

        if (Constants.IS_COUNT_TPS) {
            tpsCounter.cleanup();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 处理日志记录bolt功能,留在二期
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
