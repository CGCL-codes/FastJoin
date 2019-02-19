package soj.util;

import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;

//import backtype.storm.generated.Bolt;
//import backtype.storm.generated.ComponentCommon;
//import backtype.storm.generated.SpoutSpec;
//import backtype.storm.generated.StormTopology;

public class LogHelpers
{
    public static void logTopology(Logger logger, StormTopology t) {
        logger.info("number of spouts: " + t.get_spouts_size());

        for (Map.Entry<String, SpoutSpec> kv : t.get_spouts().entrySet()) {
            logger.info("[Spout] " + kv.getKey() + ", "
                    + getString(kv.getValue().get_common()));
        }

        logger.info("number of bolts: " + t.get_bolts_size());

        for (Map.Entry<String, Bolt> kv : t.get_bolts().entrySet()) {
            logger.info("[Bolt] " + kv.getKey() + ", "
                    + getString(kv.getValue().get_common()));
        }
    }

    private static String getString(ComponentCommon common) {
        StringBuilder sb = new StringBuilder();

        sb.append("parallelism:" + common.get_parallelism_hint());
        sb.append(", inputs:" + common.get_inputs());
        sb.append(", streams:" + common.get_streams());

        return sb.toString();
    }
}