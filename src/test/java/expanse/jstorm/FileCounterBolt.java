package expanse.jstorm;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class FileCounterBolt implements IRichBolt, Serializable {

    private static final long   serialVersionUID = -4048353193942071929L;
    private static final Logger log              = LoggerFactory.getLogger(FileCounterBolt.class);

    private Long                count            = 0L;

    private File                file;

    private OutputCollector     collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        File temp = new File(map.get("countFilePath").toString());
        if (!temp.exists()) {
            try {
                boolean boo = temp.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.file = temp;
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int length = tuple.getInteger(0);

        count += length;

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        try {
            Files.append(count.toString() + "\n", file, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
