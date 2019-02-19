package soj.biclique.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;

import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static soj.biclique.KafkaTopology.MOINTOR_R_TO_JOINER_STREAM_ID;
import static soj.biclique.KafkaTopology.MOINTOR_S_TO_JOINER_STREAM_ID;
public class MonitorBolt extends BaseRichBolt{
    private static final List<String> TO_JOINER_SCHEMA = ImmutableList.of("relation", "target",
            "rmax", "smax", "rmin", "smin");
    private int _tid;
    private FileWriter _output;
    private String _rel;
    private int _boltsNum;
    private Map<Integer, Long> _relCounter;
    private Map<Integer, Long> _oppRelCounter;
    private Map<Integer, FileWriter> _loadOutput;
    private long _lastOutputTime;
    private Stopwatch _stopwatch;
    private long _startTime;
    private int _startMonitorTime;
    private double _threshold;
    private double _interval;
    OutputCollector _collector;
    public MonitorBolt(String rel, int boltsNum, int startTime, double threshold, double interval) {
        _rel = rel;
        _boltsNum = boltsNum;
        _relCounter = new HashedMap();
        _oppRelCounter = new HashedMap();
        _loadOutput = new HashedMap();
        _startMonitorTime = startTime;
        _threshold = threshold;
        _interval = interval;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _tid = context.getThisTaskId();
        String prefix = "zsj_monitor_" + _rel.toLowerCase() + _tid;
        _output = new FileWriter("/tmp/", prefix, "txt");//.setFlushSize(10).setPrintStream(System.out);
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(MILLISECONDS);
        _collector = collector;
        _startTime = _stopwatch.elapsed(MILLISECONDS);
    }

    @Override
    public void execute(Tuple input) {
        String type = input.getStringByField("type");
        //record signal
        if(type.equals("recordSignal")){
            String relation = input.getStringByField("relation");
            String key = input.getStringByField("key");
            //Integer target = input.getIntegerByField("target");
            List<Integer> targets = new ArrayList<Integer>();//(List<Integer>) input.getValueByField("target");
            {
                String targetString = input.getStringByField("target");
                targetString = targetString.substring(1, targetString.length() - 1);
                String[] targetIDs = targetString.split(",");
                for(String target : targetIDs) {
                    targets.add(Integer.parseInt(target.trim()));
                }
            }
            for(Integer target : targets){
                if(!_loadOutput.containsKey(target)){
                    _loadOutput.put(target, new FileWriter("/tmp/", "zsj_"+ target, "txt"));
                }
            }
            if(relation.equals(_rel)){
                for(Integer target : targets){
                    if(!_relCounter.containsKey(target)){
                        _relCounter.put(target, 0L);
                        if(!_oppRelCounter.containsKey(target)){
                            _oppRelCounter.put(target, 0L);
                        }
                    }
                    _relCounter.put(target, _relCounter.get(target) + 1L);
                }
            }
            else{
                for(Integer target : targets){
                    if(!_oppRelCounter.containsKey(target)){
                        _oppRelCounter.put(target, 0L);
                        if(!_relCounter.containsKey(target)){
                            _relCounter.put(target, 0L);
                        }
                    }
                    _oppRelCounter.put(target, _oppRelCounter.get(target) + 1L);
                }
            }
            long curr = _stopwatch.elapsed(MILLISECONDS);
            if(curr - _lastOutputTime >= _interval){
                Integer maxTaskId = -1;
                Integer minTaskId = -1;
                Long maxLoad = 0L;
                Long minLoad = 0L;
                for (Map.Entry<Integer, Long> e : _relCounter.entrySet()){
                    if(maxTaskId == -1){
                        maxTaskId = e.getKey();
                        minTaskId = e.getKey();
                        maxLoad = _relCounter.get(maxTaskId) * _oppRelCounter.get(maxTaskId);
                        minLoad = _relCounter.get(minTaskId) * _oppRelCounter.get(minTaskId);
                    }
                    else{
                        Long load = e.getValue() * _oppRelCounter.get(e.getKey());
                        if(minLoad > load){
                            minTaskId = e.getKey();
                            minLoad = load;
                        }
                        if(maxLoad < load){
                            maxTaskId = e.getKey();
                            maxLoad = load;
                        }
                    }
                }
                for(Map.Entry<Integer, FileWriter> e : _loadOutput.entrySet()){
                    e.getValue().writeImmediately("" + (_relCounter.get(e.getKey()) * _oppRelCounter.get(e.getKey())));
                }
                if(curr - _startTime >= _startMonitorTime){

                    if(minLoad == 0L){
                        output((maxLoad) + ":" + minLoad);
                    }
                    if(minLoad > 0L){
                        double LI = (double)maxLoad / (double)minLoad;
                        for (Map.Entry<Integer, Long> e : _relCounter.entrySet()) {
                            output(e.getKey() + ", " + e.getValue() + ", " + _oppRelCounter.get(e.getKey()));
                        }
                        output((maxLoad) + ":" + minLoad + " " + LI);
                        if(LI > _threshold){
                            String streamId = MOINTOR_R_TO_JOINER_STREAM_ID;
                            if(_rel.equals("S")){
                                streamId = MOINTOR_S_TO_JOINER_STREAM_ID;
                            }
                            _collector.emitDirect(maxTaskId, streamId, new Values("migration", minTaskId,
                                    _relCounter.get(maxTaskId), _oppRelCounter.get(maxTaskId),
                                    _relCounter.get(minTaskId), _oppRelCounter.get(minTaskId)));

                        }
                    }
                }

                _lastOutputTime = curr;
                //update
                for (Integer e : _oppRelCounter.keySet()){
                    _oppRelCounter.put(e, _oppRelCounter.get(e) / 2);
                }
            }
        }
        //migration end signal
        else if(type.equals("migrationEndSignal")){
            Integer sid = input.getIntegerByField("sourceId");
            Integer tid = input.getIntegerByField("targetId");
            Long num = input.getLongByField("num");
            _relCounter.put(sid, _relCounter.get(sid) - num);
            _relCounter.put(tid, _relCounter.get(sid) + num);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(MOINTOR_R_TO_JOINER_STREAM_ID, new Fields(TO_JOINER_SCHEMA));
        declarer.declareStream(MOINTOR_S_TO_JOINER_STREAM_ID, new Fields(TO_JOINER_SCHEMA));
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }
}
