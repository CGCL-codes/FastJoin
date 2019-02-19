package soj.biclique.bolt;

import java.text.DecimalFormat;
import java.util.*;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import javafx.scene.layout.Priority;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import static com.google.common.collect.Lists.newLinkedList;
import soj.util.FileWriter;
import soj.util.Stopwatch;
import static soj.util.CastUtils.getBoolean;
import static soj.util.CastUtils.getInt;
import static soj.util.CastUtils.getList;
import static soj.util.CastUtils.getLong;
import static soj.util.CastUtils.getString;
import static soj.biclique.KafkaTopology.JOINER_TO_POST_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_JOINER_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_RESHUFFLER_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_MONITOR_STREAM_ID;
public class JoinBolt extends BaseBasicBolt
{
    private static final List<String> TO_POST_SCHEMA = ImmutableList.of("currentMoment",
            "tuples", "joinTimes", "processingDuration", "latency", "migrationTime", "migrationTuples", "resultNum");

    private static final List<String> TO_JOINER_SCHEMA = ImmutableList.of("relation", "timestamp", "key", "value", "type");
    private static final List<String> TO_RESHUFFLER_SCHEMA = ImmutableList.of("relation", "rel", "key", "target");
    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "sourceId", "targetId", "num");
    private static final long PROFILE_REPORT_PERIOD_IN_SEC = 1;
    private static final int BYTES_PER_TUPLE_R = 64;
    private static final int BYTES_PER_TUPLE_S = 56;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private static final Logger LOG = getLogger(JoinBolt.class);

    private final String _rel;

    public JoinBolt(String relation) {
        super();
        _rel = relation;

        checkState(_rel.equals("R") || _rel.equals("S"), "Unknown relation: "
                + _rel);
    }

    private int _tid;

    private FileWriter _output;

    private int _thisJoinFieldIdx;
    private int _oppJoinFieldIdx;
    private String _operator;

    private boolean _window;
    private long _thisWinSize;
    private long _oppWinSize;

    private int _subindexSize;
    private Queue<Pair> _indexQueue;
    private ArrayList<Values> _currList;
    private Map<String, Long>  _relKeyCounter;
    private Map<String, Long>  _oppRelKeyCounter;
    private Map<Integer, ArrayList<Values>> _buffer;

    private long _profileReportInSeconds;
    private long _triggerReportInSeconds;
    private Stopwatch _stopwatch;
    private DecimalFormat _df;

    private long _tuplesStored;
    private long _tuplesJoined;
    private long _joinedTime;
    private long _lastJoinedTime;
    private long _lastOutputTime;
    private long _lastProcessTuplesNum;
    private long _lastTuplesJoined;
    private double _latency;
    private boolean _begin;
    private int _thisTupleSize;
    private int _oppTupleSize;
    private long _resultNum;
    private long _lastResultNum;
    private long _migrationTimes;
    private long _migrationTuples;
    private long _lastReceiveTuples;
    private long _currReceiveTuples;
    private TopologyContext _context;
    @Override
    public void prepare(Map conf, TopologyContext context) {
        _tid = context.getThisTaskId();
        _context = context;
        String prefix = "zsj_joiner_" + _rel.toLowerCase() + _tid;
        _output = new FileWriter("/tmp/", prefix, "csv").setFlushSize(10);//..setPrintStream(System.out);

        _subindexSize = getInt(conf.get("subindexSize"));

        LOG.info("relation:" + _rel + ", join_field_idx(this):"
                + _thisJoinFieldIdx + ", join_field_idx(opp):"
                + _oppJoinFieldIdx + ", operator:" + _operator + ", window:"
                + _window + ", win_size:" + _thisWinSize + ", subindex_size:"
                + _subindexSize);

        /* indexes */
        _indexQueue = newLinkedList();
        _currList = new ArrayList<Values>();
        /* profiling */
        _tuplesStored = 0;
        _tuplesJoined = 0;
        _joinedTime = 0;
        _lastJoinedTime = 0;
        _lastOutputTime = 0;
        _lastProcessTuplesNum = 0;
        _latency = 0;
        _begin = true;
        _lastTuplesJoined = 0;
        _resultNum = 0;
        _lastResultNum = 0;

        _df = new DecimalFormat("0.00");
        _profileReportInSeconds = PROFILE_REPORT_PERIOD_IN_SEC;
        _triggerReportInSeconds = _profileReportInSeconds;
        _stopwatch = Stopwatch.createStarted();
        _relKeyCounter = new HashMap<String, Long>();
        _oppRelKeyCounter = new HashMap<String, Long>();
        _migrationTimes = 0L;
        _migrationTuples = 0L;
        _lastReceiveTuples = 0L;
        _currReceiveTuples = 0L;
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String rel = tuple.getStringByField("relation");

        //开始迁移
        if(rel.equals("migration")){
            _migrationTimes++;
            Long rmax = tuple.getLongByField("rmax");
            Long rmin = tuple.getLongByField("rmin");
            Long smax = tuple.getLongByField("smax");
            Long smin = tuple.getLongByField("smin");
            Integer targetId = tuple.getIntegerByField("target");
            //greedyfit
            Long loadGap = rmax * smax - rmin * smin;
            PriorityQueue<Pair> pq = new PriorityQueue<Pair>(new Comparator<Pair>() {
                @Override
                public int compare(Pair o1, Pair o2) {
                    if((Long)o1.getLeft() > (Long)o2.getLeft()){
                        return -1;
                    }
                    else if((Long)o1.getLeft() < (Long)o2.getLeft()){
                        return 1;
                    }
                    return 0;
                }
            });
            for (String key : _relKeyCounter.keySet()){
                Long mlf =  (rmax + rmin) * _relKeyCounter.get(key) +
                        (smax + smin) * _oppRelKeyCounter.get(key);
                pq.add(ImmutablePair.of(mlf, key));
            }
            Set<String> keySet = new HashSet<String>();
            //核心代码
            for(Pair p : pq){
                if(loadGap > (Long)p.getLeft()){
                    loadGap -= (Long)p.getLeft();
                    keySet.add(p.getRight().toString());
                    //output("Load gap " + loadGap + ", current max load factor: " + (Long)p.getLeft());
                }
            }
            for(String key : keySet){
                collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("migration", _rel, key, targetId));
            }
            Long tuples = 0L;
            //迁移数据
            for (Pair pairTsIndex : _indexQueue) {
                Long ts = getLong(pairTsIndex.getLeft());
                List<Values> lst = (List<Values>)pairTsIndex.getRight();
                for(Iterator<Values> it = lst.iterator(); it.hasNext();){
                    Values v = it.next();
                    if(keySet.contains(v.get(2).toString())){
                        collector.emitDirect(targetId, JOINER_TO_JOINER_STREAM_ID, new Values(v.get(0), v.get(1), v.get(2), v.get(3), "migration"));
                        tuples++;
                        it.remove();
                    }
                }
            }
            for(Iterator<Values> it = _currList.iterator(); it.hasNext();){
                Values v = it.next();
                if(keySet.contains(v.get(2).toString())){
                    collector.emitDirect(targetId, JOINER_TO_JOINER_STREAM_ID, new Values(v.get(0), v.get(1), v.get(2), v.get(3), "migration"));
                    tuples++;
                    it.remove();
                }
            }
            collector.emitDirect(targetId, JOINER_TO_JOINER_STREAM_ID, new Values("migrationEnd","","",""));
            collector.emit(JOINER_TO_MONITOR_STREAM_ID, new Values("migrationEndSignal", _tid, targetId, tuples));
            _migrationTuples += tuples;
        }
        else if(rel.equals("migrationEnd")){
            ArrayList<Values> lst = _buffer.get(tuple.getSourceTask());
            for(Values v : lst) {
                //store
                if(v.get(0).equals(_rel)){
                    _currList.add(v);
                    if(_currList.size() >= _subindexSize){
                        _indexQueue.add((Pair)ImmutablePair.of(v.get(1), _currList));
                        _currList.clear();
                    }
                }
                //join
                else{
                    join(new TupleImpl(_context, v, 0, null, null), _currList, collector);
                }
            }
            _buffer.remove(tuple.getSourceTask());
        }
        else{
            try {
                if(tuple.getStringByField("type").equals("migration")){
                    if(!_buffer.containsKey(tuple.getSourceTask())){
                        _buffer.put(tuple.getSourceTask(), new ArrayList<Values>());
                    }
                    ArrayList<Values> lst = _buffer.get(tuple.getSourceTask());
                    lst.add(new Values(tuple.getStringByField("relation"), tuple.getLongByField("timestamp"), tuple.getStringByField("key"), tuple.getStringByField("value")));
                }
            } catch(IllegalArgumentException e){
                _currReceiveTuples++;
                if(_begin){
                    _lastOutputTime = _stopwatch.elapsed(MILLISECONDS);
                    _begin = false;
                }
                long currTime = _stopwatch.elapsed(MICROSECONDS);//System.currentTimeMillis();
                String key = tuple.getStringByField("key");
                if(!_relKeyCounter.containsKey(key)){
                    _relKeyCounter.put(key, 0L);
                }
                if(!_oppRelKeyCounter.containsKey(key)){
                    _oppRelKeyCounter.put(key, 0L);
                }
                if (rel.equals(_rel)) {
                    store(tuple);
                    ++_tuplesStored;
                    _relKeyCounter.put(key, _relKeyCounter.get(key) + 1L);
                }
                else { // rel.equals(Opp(_rel))
                    join(tuple, collector);
                    ++_tuplesJoined;
                    _oppRelKeyCounter.put(key, _oppRelKeyCounter.get(key) + 1L);
                }
                _latency += (_stopwatch.elapsed(MICROSECONDS) - currTime) / 1000.0;
            }
            if (isTimeToOutputProfile()) {
                //emit() 当前时间（秒），处理的tupls数，_latency
                long moment = _stopwatch.elapsed(SECONDS);
                long tuples = _tuplesStored + _tuplesJoined - _lastProcessTuplesNum;
                long joinTimes = _joinedTime - _lastJoinedTime;
                long processingDuration = _stopwatch.elapsed(MILLISECONDS) - _lastOutputTime;
                long resultNum = _resultNum - _lastResultNum;
                collector.emit(JOINER_TO_POST_STREAM_ID, new Values( moment, tuples, joinTimes, processingDuration, _latency, _migrationTimes, _migrationTuples, resultNum));
                output(getProfile());
            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(JOINER_TO_POST_STREAM_ID, new Fields(TO_POST_SCHEMA));
        declarer.declareStream(JOINER_TO_JOINER_STREAM_ID, new Fields(TO_JOINER_SCHEMA));
        declarer.declareStream(JOINER_TO_RESHUFFLER_STREAM_ID, new Fields(TO_RESHUFFLER_SCHEMA));
        declarer.declareStream(JOINER_TO_MONITOR_STREAM_ID, new Fields(TO_MONITOR_SCHEMA));
    }

    @Override
    public void cleanup() {
        _stopwatch.stop();

        StringBuilder sb = new StringBuilder();

        sb.append("relation:" + _rel);
        sb.append(", num_of_indexes:" + (_indexQueue.size() + 1));

        output(sb.toString());

        if (_output != null)
            _output.endOfFile();
    }

    private void store(Tuple tuple) {
        String rel = tuple.getStringByField("relation");
        Long ts = tuple.getLongByField("timestamp");
        String key = tuple.getStringByField("key");
        String value = tuple.getStringByField("value");

        Values values = new Values(rel, ts, key, value);
        _currList.add(values);
        if(_currList.size() >= _subindexSize){
            _indexQueue.add((Pair)ImmutablePair.of(ts, _currList));
            _currList.clear();
        }
    }

    private void join(Tuple tupleOpp, BasicOutputCollector collector) {
        /* join with archive indexes */
        int numToDelete = 0;
        Long tsOpp = tupleOpp.getLongByField("timestamp");
        for (Pair pairTsIndex : _indexQueue) {
            Long ts = getLong(pairTsIndex.getLeft());
            if (_window && !isInWindow(tsOpp, ts)) {
                ++numToDelete;
                continue;
            }
            join(tupleOpp, (List<Values>)pairTsIndex.getRight(), collector);
        }

        for (int i = 0; i < numToDelete; ++i) {
            _indexQueue.poll();
        }

        /* join with current index */
        //join(tupleOpp, _currMap, collector);
        join(tupleOpp, _currList, collector);
        String key = tupleOpp.getStringByField("key");
        _oppRelKeyCounter.put(key, _oppRelKeyCounter.get(key) - 1);
    }
    private void join(Tuple tupleOpp, List<Values> index, BasicOutputCollector collector){
        String key = tupleOpp.getStringByField("key");
        _joinedTime += index.size();
        for(Values record : index){
            //join successful
            if(record.get(2).equals(key)){
                _resultNum++;
                //output("match key:" + key);
                //collector.emit(new Values());

            }
        }
    }
    private void join(Tuple tupleOpp, Object index,
            BasicOutputCollector collector) {
        String key = tupleOpp.getStringByField("key");

        for (Values record : getMatchings(index, key)) {
            String value = getString(record, 2);
            // output result
            if (_rel.equals("R")) {
                output("R: " + value + " ---- " + tupleOpp.getStringByField("value"));
//                collector.emit(new Values(joinFieldValue, ts, seq, payload,
//                        tsOpp, seqOpp, payloadOpp));
            }
            else { // _rel.equals("S")
                output("S: " + tupleOpp.getStringByField("value") + " ---- " + value);
//                collector.emit(new Values(joinFieldValue, tsOpp, seqOpp,
//                        payloadOpp, ts, seq, payload));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<Values> getMatchings(Object index, Object value) {
        return ((Multimap<Object, Values>) index).get(value);
    }

    @SuppressWarnings("unchecked")
    private int getIndexSize(Object index) {
        return ((List<Values>) index).size();
    }

    @SuppressWarnings("unchecked")
    private int getNumTuplesInWindow() {
        int numTuples = 0;
        for (Pair pairTsIndex : _indexQueue) {
            numTuples += ((Multimap<Object, Values>) pairTsIndex.getRight())
                    .size();
        }
        //numTuples += _currMap.size();

        return numTuples;
    }

    private boolean isInWindow(long tsIncoming, long tsStored) {
        long tsDiff = tsIncoming - tsStored;

        if (tsDiff >= 0) {
            return (tsDiff <= _thisWinSize);
        }
        else {
            return (-tsDiff <= _oppWinSize);
        }
    }

    private boolean isTimeToOutputProfile() {
        long currTime = _stopwatch.elapsed(SECONDS);

        if (currTime >= _triggerReportInSeconds) {
            _triggerReportInSeconds = currTime + _profileReportInSeconds;
            return true;
        }
        else {
            return false;
        }
    }

    private String getProfile() {
        StringBuilder sb = new StringBuilder();
        long currTime = _stopwatch.elapsed(MILLISECONDS);
        sb.append("[Joiner-" + _rel + "-" + _tid);
        sb.append(" @ " + _stopwatch.elapsed(SECONDS) + " sec],");

        double throughput = _tuplesStored + _tuplesJoined - _lastProcessTuplesNum;
        throughput /=((double) (currTime - _lastOutputTime)) / 1000.0;
        sb.append(String.format("%.2f,", throughput));
        if(_tuplesStored + _tuplesJoined == _lastProcessTuplesNum){
            _latency = Double.MAX_VALUE;
        }
        else{
            _latency /= ((double) (_tuplesStored + _tuplesJoined - _lastProcessTuplesNum));
        }

        sb.append(String.format("%.8f,", _latency));
        double time = (_joinedTime - _lastJoinedTime) * 1.0;
        sb.append( time + "");
        sb.append("," + _tuplesStored);
        sb.append("," + (_tuplesJoined - _lastTuplesJoined));
        _lastReceiveTuples = _currReceiveTuples;
        _lastJoinedTime = _joinedTime;
        _lastOutputTime = currTime;
        _lastProcessTuplesNum = _tuplesStored + _tuplesJoined;
        _lastTuplesJoined = _tuplesJoined;
        _latency = 0;
        _begin = true;
        _lastResultNum = _resultNum;

        return sb.toString();
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }
}
