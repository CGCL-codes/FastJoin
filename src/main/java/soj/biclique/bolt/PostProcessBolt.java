package soj.biclique.bolt;

import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import static com.google.common.hash.Hashing.murmur3_32;
import static soj.util.CastUtils.getLong;

//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
import soj.util.FileWriter;
import soj.util.Stopwatch;


public class PostProcessBolt extends BaseRichBolt
{
    private static final Logger LOG = getLogger(PostProcessBolt.class);

    private static final List<String> SCHEMA = ImmutableList.of("count", "sum", "min", "max");

    private static final double PERCENTAGE_THRH_FOR_STANDBY = 0.5;
    private static final double PERCENTAGE_THRH_FOR_SWAP = 0.9;

    private OutputCollector _collector;

    private FileWriter _output;
    private long _currTime;
    // srj
    private long _lastOutputThroughTime;
    private long _lastThroughCount;
    private long _boltsNum;
    private Stopwatch _stopwatch;
    //map
    private Map<Integer, Queue<Values>> _statistics;
    public PostProcessBolt(int boltsNum){
        _boltsNum = boltsNum;
        _statistics = new HashMap<Integer, Queue<Values>>();
    }
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        _lastOutputThroughTime = System.currentTimeMillis();
        _lastThroughCount = 0;

        _collector = collector;

        String prefix = "zsj_post" + context.getThisTaskId();
        _output = new FileWriter("/tmp/", prefix, "csv");
        _stopwatch = Stopwatch.createStarted();
        _currTime = _stopwatch.elapsed(MILLISECONDS);
    }

    public void execute(Tuple tuple) {
        Values v = new Values(tuple.getLongByField("currentMoment"),
                tuple.getLongByField("tuples"), tuple.getLongByField("joinTimes"),
                tuple.getLongByField("processingDuration"), tuple.getDoubleByField("latency"),
                tuple.getLongByField("migrationTime"), tuple.getLongByField("migrationTuples"), tuple.getLongByField("resultNum"));
        int taskId = tuple.getSourceTask();
        if(!_statistics.containsKey(taskId)){
            _statistics.put(taskId, new LinkedList<>());
        }
        _statistics.get(taskId).add(v);
        Long ts = getLong(_statistics.get(taskId).peek().get(0));
        while(_statistics.size() == _boltsNum){
            for(Map.Entry<Integer, Queue<Values>> e : _statistics.entrySet()){
                Values temp = e.getValue().peek();
                Long tempTs = getLong(temp.get(0));
                boolean empty = false;
                while(tempTs.compareTo(ts) < 0){
                    e.getValue().poll();
                    if(e.getValue().isEmpty()){
                        _statistics.remove(e.getKey());
                        empty = true;
                        break;
                    }
                    temp = e.getValue().peek();
                    tempTs = getLong(temp.get(0));
                }
                if(empty){
                    break;
                }
                ts = tempTs;
            }
            if(_statistics.size() == _boltsNum) {
                boolean equals = true;
                for (Map.Entry<Integer, Queue<Values>> e : _statistics.entrySet()) {
                    Values temp = e.getValue().peek();
                    Long tempTs = getLong(temp.get(0));
                    if(!tempTs.equals(ts)){
                        equals = false;
                        break;
                    }
                }
                long curr = _stopwatch.elapsed(MILLISECONDS);
                boolean hasOutput = false;
                if(curr - _currTime >= 1000){
                    if(equals){
                        //output statistics
                        long tuples = 0;
                        long joinTimes = 0;
                        long processingDuration = 0;
                        double latency = 0;
                        long migrationTimes = 0;
                        long migrationTuples = 0;
                        LinkedList<Integer> lst = new LinkedList<Integer>();
                        int num = 0;
                        long resultNum = 0;
                        for (Map.Entry<Integer, Queue<Values>> e : _statistics.entrySet()) {
                            if(!e.getValue().isEmpty()){
                                Values temp = e.getValue().poll();
                                if(e.getValue().isEmpty()){
                                    lst.add(e.getKey());
                                }
                                tuples += getLong(temp.get(1));
                                joinTimes += getLong(temp.get(2));
                                processingDuration += getLong(temp.get(3));
                                latency += ((Double)temp.get(4)).doubleValue();
                                migrationTimes += getLong(temp.get(5));
                                migrationTuples += getLong(temp.get(6));
                                resultNum += getLong(temp.get(7));
                                num++;
                            }
                        }
                        for(Integer i : lst){
                            _statistics.remove(i);
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append("@ [" + ts + " sec]");
                        double throughput = tuples;
                        //double throughput = resultNum;
                        if(processingDuration == 0){
                            throughput = 0;
                        }
                        else{
                            //processingDuration
                            throughput /= processingDuration;
                            throughput *= 1000;
                            throughput *= num;
                        }
                        sb.append(String.format(", %.2f", throughput));
                        if(tuples == 0){
                            latency = 0;
                        }
                        else{
                            latency /= tuples;
                        }
                        sb.append(String.format(", %.8f", latency));
                        output(sb.toString());
                        hasOutput = true;
                    }
                }
                if(hasOutput){
                    _currTime = _stopwatch.elapsed(MILLISECONDS);
                }
            }
        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SCHEMA));
    }

    @Override
    public void cleanup() {
        if (_output != null)
            _output.endOfFile();
    }
    private void output(String msg) {
        if (_output != null){
            _output.writeImmediately(msg);
        }
    }


    private void outputSimple(String joinVal, String seq) {
        output(seq + " | " + joinVal);
    }

    private void outputLatency(Long tsR, Long tsS) {
        long tsMoreRecent = Math.max(tsR, tsS);
        long tsJoinResult = System.currentTimeMillis();
        output("Latency: " + (tsJoinResult - tsMoreRecent) + " ms");
    }

}