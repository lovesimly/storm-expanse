package expanse.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class FileSport implements IRichSpout, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSport.class);

    private static final long serialVersionUID = 868942976113897053L;

    private SpoutOutputCollector collector;

    private FileReader fileReader;

    private boolean completed = false;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.fileReader = new FileReader(map.get("filePath").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = spoutOutputCollector;

    }

    @Override
    public void close() {
        if (fileReader != null) {
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        System.out.println("nextTuple execute...");

        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String str;
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        try {
            while ((str = bufferedReader.readLine()) != null) {
                System.out.println(("current str=" + str));

                if (str.length() <= 0) {
                    System.out.println("sleep.....");
                    Thread.sleep(10000);

                }
                this.collector.emit(new Values(str), str);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            completed = true;
        }

    }

    @Override
    public void ack(Object o) {
        System.out.println("ACK OK:" + o);
    }

    @Override
    public void fail(Object o) {
        System.out.println("ACK FAIL:" + o);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
