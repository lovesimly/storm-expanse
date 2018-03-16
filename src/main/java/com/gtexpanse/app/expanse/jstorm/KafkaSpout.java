package com.gtexpanse.app.expanse.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.gtexpanse.app.expanse.bean.DBMSAction;
import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.bean.MessageTuple;
import com.gtexpanse.app.expanse.utils.Constants;
import com.gtexpanse.app.expanse.utils.TpsCounter;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * kafka spout --
 * <p> 消费kafka消息，组装MessageTuple丢入BlockingQueue，并发射给bolt节点
 * <p> spout中<b>不应该</b>有过滤掉无需处理消息的功能，这块请业务方自行在handler中处理，尽管这样确实加重了jstorm的工作
 */
public class KafkaSpout implements IRichSpout, IAckValueSpout, IFailValueSpout {

    private static final long serialVersionUID = -5234123834136869893L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    /**
     * 独立日志输出
     */
    private static final Logger LOG_KAFKA = LoggerFactory.getLogger("EXPANSE-KAFKA");
    private static final Logger LOG_ACK = LoggerFactory.getLogger("EXPANSE-ACK");
    private static final Logger LOG_FAIL = LoggerFactory.getLogger("EXPANSE-FAIL");


    private ExpanseConfig config;

    private SpoutOutputCollector collector;

    private transient BlockingQueue<MessageTuple> sendingQueue;

    private transient Consumer consumer;

    private TpsCounter tpsCounter;

    public KafkaSpout(ExpanseConfig config) {
        super();
        this.config = config;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("Kafka Spout opening begin...");

        this.collector = collector;

        if (Constants.IS_COUNT_TPS) {
            tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());
        }

        this.sendingQueue = new ArrayBlockingQueue<>(config.getStorm().getMaxPendingCount());

        initKafkaConsumer();
    }

    private void initKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafka().getBrokers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getKafka().getGroupId());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        //        // kafka一次拉取的最大消息数目
        //        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        //                String.valueOf(config.getStorm().getMaxPendingCount()));

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(config.getKafka().getTopic()),new MessageConsumer());
//        KafkaFactory factory = new ZAKafkaFactory();
//        consumer = factory.createConsumer(properties);
//        consumer.subscribe(config.getKafka().getTopic(), new MessageConsumer());
//        consumer.start();
    }

    @Override
    public void nextTuple() {
        MessageTuple messageTuple = null;
        try {
            messageTuple = sendingQueue.take();
        } catch (InterruptedException e) {
        }

        if (messageTuple == null) {
            return;
        }

        sendTuple(messageTuple);
    }

    private void sendTuple(MessageTuple messageTuple) {
        Object id = messageTuple.getMessage().get(config.getIndexMappingId());
        if (id == null) {
            id = messageTuple.getMessage().get("id");
        }

        // 发射
        messageTuple.updateEmitMs();

        collector.emit(new Values(id, messageTuple), messageTuple.getMsgId());

        if (Constants.IS_COUNT_TPS) {
            tpsCounter.count();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "MessageTuple"));
    }

    @Override
    public void ack(Object msgId, List<Object> values) {
        MessageTuple messageTuple = (MessageTuple) values.get(1);
        messageTuple.done();

        LOG_ACK.warn(config.getStorm().getTopologyName() + " - " + messageTuple);
    }

    @Override
    public void fail(Object msgId, List<Object> values) {
        MessageTuple messageTuple = (MessageTuple) values.get(1);

        LOG_FAIL.warn(config.getStorm().getTopologyName() + " - " + messageTuple);

        AtomicInteger failTimes = messageTuple.getFailureTimes();
        int failNum = failTimes.incrementAndGet();
        // 超过限定的重发次数
        if (failNum > Constants.FAIL_RETRY_TIMES) {
            // 丢回给kafka
            messageTuple.fail();
            return;
        }

        try {
            // 丢入发送队列，等待10毫秒，否则自己发送
            boolean boo = sendingQueue.offer(messageTuple, 100, TimeUnit.MILLISECONDS);
            if (!boo) {
                sendTuple(messageTuple);
            }
        } catch (InterruptedException e) {
            LOG.warn("Check this InterruptedException where spout-fail,", e);
        }
    }

    @Override
    @Deprecated
    public void ack(Object msgId) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    @Deprecated
    public void fail(Object msgId) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }

        if (Constants.IS_COUNT_TPS) {
            tpsCounter.cleanup();
        }
        LOG.info("kafka consumer has shutdown...");
    }

    @Override
    public void activate() {
        LOG.info("kafka spout activate, start consumers...");
    }

    @Override
    public void deactivate() {
        LOG.info("kafka spout deactivate...");
    }

    /**
     * 消息反序列化
     */
    private MessageTuple buildMessageTuple(ConsumerRecord<String, byte[]> record) {
        try {
            // todo deSerialized your msg ...
//                Map<String, Object> recordMap = record.value();
            Map<String, Object> recordMap = new HashMap<>();
            if (MapUtils.isEmpty(recordMap)) {
                return null;
            }

            Map<String, Object> transMap = new HashMap<>();
            // 只发送需要的字段给bolt,减少网络流量
            for (String field : config.getTransferFields()) {
                transMap.put(field, recordMap.get(field));
            }
            // todo chose your dbms action from msg
            return new MessageTuple(record.key(), DBMSAction.INSERT.getValue(), transMap);
        } catch (Exception e) {
            LOG_KAFKA.error("kafka key:" + record.key() + " decode message error:", e);
            return null;
        }
    }

    /**
     * kafka MessageConsumer
     */
    class MessageConsumer implements ConsumerRebalanceListener {

        MessageConsumer() {
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        }

//        @Override
//        public Action consume(ConsumerRecord<String, byte[]> record, ConsumeContext consumeContext) {
//            if (record == null) {
//                return Action.CommitMessage;
//            }
//
//            LOG_KAFKA.info("[RECEIVE-MSG] - {topic = " + record.topic() + ", partition = " + record.partition()
//                    + ", offset = " + record.offset() + ", key = " + record.key() + "}");
//
//            try {
//                MessageTuple tuple = buildMessageTuple(record);
//                if (tuple == null) {
//                    return Action.ReconsumeLater;
//                }
//
//                boolean boo = sendingQueue.offer(tuple);
//                if (!boo) {
//                    return Action.ReconsumeLater;
//                }
//
//                // latch await
//                tuple.waitForHandle();
//                if (tuple.isSuccess()) {
//                    return Action.CommitMessage;
//                } else {
//                    // 消费时候后端出问题了,让kafka再推送一次。注意：ack先会重试三次，三次之后还是失败才会交给kafka重试
//                    LOG_KAFKA.error("consume and handler msg failed : messageTuple : " + tuple);
//                    return Action.ReconsumeLater;
//                }
//            } catch (Exception e) {
//                LOG_KAFKA.error("consume kafka msg occur exception:", e);
//                return Action.ReconsumeLater;
//            }
    }


}
