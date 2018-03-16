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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FileBolt implements IRichBolt, Serializable {

    private static final Logger log              = LoggerFactory.getLogger(FileBolt.class);

    private static final long   serialVersionUID = 2098494532852549233L;
    private Integer             id;
    private String              name;
    private OutputCollector     collector;
    private File                file;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();

        File temp = new File(map.get("newFilePath").toString());
        if (!temp.exists()) {
            try {
                boolean boo = temp.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.file = temp;
    }

    @Override
    public void execute(Tuple tuple) {
        String strLine = tuple.getString(0).trim();

        log.info("FileBolt begin:name=" + name + ",id=" + id + ",str=" + strLine);

        StringBuilder newStr = new StringBuilder();
        for (int i = 0; i < strLine.length(); i++) {
            char s = strLine.charAt(i);
            newStr.append(s).append(" ");
        }

        try {
            Files.append(newStr.toString().trim() + "\n", file, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }

        int len = strLine.length();
        // 提交给下一个bolt，用来统计字符总数
        collector.emit(new Values(len));

        // ack
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
