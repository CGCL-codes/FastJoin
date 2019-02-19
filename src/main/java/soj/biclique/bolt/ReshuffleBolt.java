package soj.biclique.bolt;

import java.util.*;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import soj.util.FileWriter;
import soj.util.Stopwatch;

import static soj.biclique.KafkaTopology.BROADCAST_R_STREAM_ID;
import static soj.biclique.KafkaTopology.BROADCAST_S_STREAM_ID;
import static soj.biclique.KafkaTopology.SHUFFLE_R_STREAM_ID;
import static soj.biclique.KafkaTopology.SHUFFLE_S_STREAM_ID;
import static soj.biclique.KafkaTopology.MOINTOR_R_STREAM_ID;
import static soj.biclique.KafkaTopology.MOINTOR_S_STREAM_ID;
import static soj.util.CastUtils.getList;


import java.util.concurrent.TimeUnit;


public class ReshuffleBolt extends BaseRichBolt
{
    private static final List<String> SCHEMA = ImmutableList.of("relation", "timestamp", "key", "value", "target");
    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "relation", "key", "target");
    private static final String _streamShuffleR = SHUFFLE_R_STREAM_ID;
    private static final String _streamShuffleS = SHUFFLE_S_STREAM_ID;
    private static final String _streamBroadcastR = BROADCAST_R_STREAM_ID;
    private static final String _streamBroadcastS = BROADCAST_S_STREAM_ID;
    private static final String _streamMonitorR = MOINTOR_R_STREAM_ID;
    private static final String _streamMonitorS = MOINTOR_S_STREAM_ID;
    private HashFunction h = Hashing.murmur3_128(13);
    private Stopwatch _stopwatch;
    private OutputCollector _collector;
    private FileWriter _output;
    private Map<String, Integer> _rRouterTable;
    private Map<String, Integer> _sRouterTable;
    private String _type;
    private long _lastOutputTime;
    private long _r;
    private long _s;
    public ReshuffleBolt(String type){
        _rRouterTable = new HashMap<String, Integer>();
        _sRouterTable = new HashMap<String, Integer>();
        _type = type;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        _output = new FileWriter("/tmp/", "zsj_reshuffle" + topologyContext.getThisTaskId(), "txt").setFlushSize(1);
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
        _r = 0;
        _s = 0;
    }
    @Override
    public void execute(Tuple tuple) {
        /* extract contents from the tuple */
        String rel = tuple.getStringByField("relation");

        if(rel.equals("migration")){
            String r = tuple.getStringByField("rel");
            String key = tuple.getStringByField("key");
            Integer target = tuple.getIntegerByField("target");
            if(r.equals("R")){
                _rRouterTable.put(key, target);
            }
            else{
                _sRouterTable.put(key, target);
            }
        }
        else{
            Long ts = tuple.getLongByField("timestamp");
            String key = tuple.getStringByField("key");
            String value = tuple.getStringByField("value");
            Integer target = -1;
            if (_type.equals("fastjoin")) {
                if(rel.equals("R")){
                    if(_rRouterTable.containsKey(key)){
                        target = _rRouterTable.get(key);
                    }
                }
                else{
                    if(_sRouterTable.containsKey(key)){
                        target = _sRouterTable.get(key);
                    }
                }
            }
            Values values = new Values(rel, ts, key, value, target);
            /* shuffle to store and broadcast to join */
            if (rel.equals("R")) {
                List<Integer> taskRId = _collector.emit(_streamShuffleR, values);
                List<Integer> taskSId = _collector.emit(_streamBroadcastR, values);
                _collector.emit(_streamMonitorR, new Values("recordSignal", rel, key, taskRId.toString()));
                _collector.emit(_streamMonitorS, new Values("recordSignal", rel, key, taskSId.toString()));
                _r++;
            }
            else { // rel.equals("S")
                List<Integer> taskSId = _collector.emit(_streamShuffleS, values);
                List<Integer> taskRId = _collector.emit(_streamBroadcastS, values);
                _collector.emit(_streamMonitorS, new Values("recordSignal", rel, key, taskSId.toString()));
                _collector.emit(_streamMonitorR, new Values("recordSignal", rel, key, taskRId.toString()));
                _s++;
            }
            // long currTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
            // if(currTime > _lastOutputTime + 2000){
            //     //output("r:" + _r + " s:" + _s);
            //     _lastOutputTime = currTime;
            // }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(_streamShuffleR, new Fields(SCHEMA));
        declarer.declareStream(_streamShuffleS, new Fields(SCHEMA));
        declarer.declareStream(_streamBroadcastR, new Fields(SCHEMA));
        declarer.declareStream(_streamBroadcastS, new Fields(SCHEMA));
        declarer.declareStream(_streamMonitorR, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(_streamMonitorS, new Fields(TO_MONITOR_SCHEMA));
    }
    private void output(String msg) {
        if (_output != null)
            //_output.write(msg);
            _output.writeImmediately(msg);
    }
    @Override
    public void cleanup() {
        if (_output != null) {
            _output.endOfFile();
        }
    }


}