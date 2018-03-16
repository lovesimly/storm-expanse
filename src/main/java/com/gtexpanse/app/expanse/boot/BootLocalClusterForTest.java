package com.gtexpanse.app.expanse.boot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.jstorm.KafkaBolt;
import com.gtexpanse.app.expanse.jstorm.KafkaSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * storm-local-cluster模式启动类
 * <p>
 * 可在本地模拟一个单机版的jStorm增量程序
 */
public class BootLocalClusterForTest {

    private static final Logger log = LoggerFactory.getLogger(BootLocalClusterForTest.class);

    public static void main(String[] args) {
        ExpanseConfig config = null;
        try {
            // MUST specify config file first ------------
            config = ExpanseConfig.loadConfig("/test/search4user.json");
            // MUST specify config file first ------------
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (config == null) {
            log.error("config is null");
            return;
        }

        LocalCluster cluster = new LocalCluster();

        String topoName = config.getStorm().getTopologyName();

        // Set storm configs
        HashMap<String, Object> stormConf = new HashMap<>();
        stormConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, config.getStorm().getMaxPendingCount());
        stormConf.put(Config.TOPOLOGY_WORKERS, config.getStorm().getWorkerCount());

        // Create topology
        TopologyBuilder builder = new TopologyBuilder();

        // spout 个数
        int spout_Parallelism_hint = config.getStorm().getSpoutCount();
        // bolt 个数
        int bolt_Parallelism_hint = config.getStorm().getBoltCount();

        builder.setSpout(topoName + "_spout", new KafkaSpout(config), spout_Parallelism_hint);

        builder.setBolt(topoName + "_bolt", new KafkaBolt(config), bolt_Parallelism_hint)
                .fieldsGrouping(topoName + "_spout", new Fields("id"));

        log.warn(">>>> [STORM-EXPANSE] - Kafka topology build success >>> now begin to submit topology : topoName = "
                + topoName + ", spoutCount = " + spout_Parallelism_hint + ", boltCount = " + bolt_Parallelism_hint
                + "<<<<");

        // 提交Local Cluster
        cluster.submitTopology(topoName, stormConf, builder.createTopology());

        try {
            Thread.sleep(600 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology(topoName);
        cluster.shutdown();

        System.exit(0);
    }
}
