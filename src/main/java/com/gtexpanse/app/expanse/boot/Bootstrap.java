package com.gtexpanse.app.expanse.boot;

import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.jstorm.KafkaTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * es 实时增量程序启动入口 - storm-cluster模式启动类
 *
 * jstorm jar xxx.jar Bootstrap
 */
public class Bootstrap {

    private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            log.error("The bootstrap program needs parameters like : search4user.js");
            return;
        }
        for (String arg : args) {
            try {
                ExpanseConfig conf = ExpanseConfig.loadConfig(arg);
                KafkaTopology topology = new KafkaTopology(conf);
                topology.start();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
