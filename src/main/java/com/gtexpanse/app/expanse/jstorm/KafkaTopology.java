package com.gtexpanse.app.expanse.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class KafkaTopology {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopology.class);

    private static final String STORM_CLUSTER_MODE = "distributed";

    /**
     * acker 个数
     */
    private static final int ACKER_COUNT = 1;

    /**
     * 项目配置
     */
    private ExpanseConfig config;

    public KafkaTopology(ExpanseConfig config) {
        super();
        this.config = config;
    }

    public void start() throws AlreadyAliveException, InvalidTopologyException {

        log.warn(">>>> [STORM-EXPANSE] - Kafka topology build start <<<<");

        String topoName = config.getStorm().getTopologyName();

        // Set storm configs
        HashMap<String, Object> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, ACKER_COUNT);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, config.getStorm().getMaxPendingCount());
        stormConf.put(Config.TOPOLOGY_WORKERS, config.getStorm().getWorkerCount());
        stormConf.put(Config.STORM_CLUSTER_MODE, STORM_CLUSTER_MODE);

        // Create topology
        TopologyBuilder builder = new TopologyBuilder();

        // spout 个数
        int spout_Parallelism_hint = config.getStorm().getSpoutCount();
        // bolt 个数
        int bolt_Parallelism_hint = config.getStorm().getBoltCount();

        String spoutName = topoName + "_spout";
        builder.setSpout(spoutName, new KafkaSpout(config), spout_Parallelism_hint);

        builder.setBolt(topoName + "_bolt", new KafkaBolt(config), bolt_Parallelism_hint)
                .fieldsGrouping(spoutName, new Fields("id"));

        log.warn(">>>> [STORM-EXPANSE] - Kafka topology build success >>> there begin to submit topology: topoName = "
                + topoName + ", spoutCount = " + spout_Parallelism_hint + ", boltCount = " + bolt_Parallelism_hint
                + "<<<<");

        // 提交整个Topology
        StormSubmitter.submitTopology(topoName, stormConf, builder.createTopology());
    }
}
