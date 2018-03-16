package com.gtexpanse.app.expanse.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.bean.MessageTuple;
import com.gtexpanse.app.expanse.es.ElasticSearchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * log bolt:用来把所有的事件记录到log es集群，方便查询和回流
 * <p>
 * 该bolt暂时未用，等待后续监控需求扩展吧
 * </p>
 */
public class LogBolt implements IRichBolt {
    public static final Logger log = LoggerFactory.getLogger(LogBolt.class);
    private static final long serialVersionUID = -5452328665129906235L;

    private OutputCollector collector;

    private ExpanseConfig conf;
    private ElasticSearchClient esClient;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public LogBolt(ExpanseConfig conf) {
        super();
        this.conf = conf;
    }

    @Override
    public void cleanup() {
        log.info("bolt cleanup...");
        if (esClient != null) {
            esClient.close();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        MessageTuple messageTuple = (MessageTuple) tuple.getValueByField("MessageTuple");
        Map<String, Object> transMap = messageTuple.getMessage();
        String index = conf.getEsLog().getIndex() + "-" + dateFormat.format(new Date());
        String type = conf.getEsLog().getType();
        transMap.put("ons_msgId", messageTuple.getMsgId());
        transMap.put("db_action_flag", messageTuple.getDbActionFlag());
        try {
//            esClient.autoCreateDocument(index, type, transMap);
            collector.ack(tuple);
        } catch (Exception e) {
            log.error("es auto create document occur error:", e);
            collector.fail(tuple);
        }
    }

    @Override
    public void prepare(Map arg0, TopologyContext topoContext, OutputCollector collector) {
        this.collector = collector;
        //init es log
        esClient = new ElasticSearchClient(conf.getEsLog().getClusterName(), conf.getEsLog().getClusterUrls());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
