package soj.util;

//import backtype.storm.Config;
//import backtype.storm.LocalCluster;
//import static backtype.storm.StormSubmitter.submitTopology;
import static org.apache.storm.StormSubmitter.submitTopology;

//import backtype.storm.generated.StormTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

public final class StormRunner
{
    private static final int MILLIS_IN_SEC = 1000;

    public static void runLocally(String topologyName, StormTopology topology,
                                  Config conf, int runtimeInSeconds) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    public static void runInCluster(String topologyName,
            StormTopology topology, Config conf) throws Exception {
        submitTopology(topologyName, conf, topology);
    }
}