package expanse.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * created by gtexpanse
 */
public class FileTopology {

    /**
     * storm local cluster test
     * 
     * @param args
     */
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new FileSport());

        BoltDeclarer boltDeclarer = builder.setBolt("word-normalizer", new FileBolt())
                .localOrShuffleGrouping("word-reader");
        boltDeclarer.fieldsGrouping("word-normalizer", new Fields("id"));

        builder.setBolt("word-counter", new FileCounterBolt()).localOrShuffleGrouping("word-normalizer");

        Config conf = new Config();
        conf.put("filePath", "E:\\storm\\text.txt");
        conf.put("newFilePath", "E:\\storm\\new_file.txt");
        conf.put("countFilePath", "E:\\storm\\counter.txt");
        conf.setMaxSpoutPending(1);
        conf.setNumWorkers(4);
        conf.setNumAckers(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
    }
}
