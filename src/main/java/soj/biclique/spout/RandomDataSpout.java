package soj.biclique.spout;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import static java.util.UUID.randomUUID;
import java.util.concurrent.ConcurrentMap;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import static com.google.common.collect.Lists.newArrayList;

//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
import static org.apache.storm.utils.Utils.sleep;
import soj.util.FileWriter;
import soj.util.Stopwatch;
import static soj.util.CastUtils.getBoolean;
import static soj.util.CastUtils.getInt;
import static soj.util.CastUtils.getLong;
import static soj.util.CastUtils.getString;

public class RandomDataSpout extends BaseRichSpout
{
    private static final List<String> SCHEMA = ImmutableList.of("relation",
            "timestamp", "seq", "payload");

    private static final long PROFILE_REPORT_PERIOD_IN_SEC = 1;
    
    private static final Logger LOG = getLogger(RandomDataSpout.class);

    private final String _rel;

    public RandomDataSpout() {
        super();
        _rel = "";
    }

    public RandomDataSpout(String relation) {
        super();
        _rel = relation;

        checkState(_rel.equals("R") || _rel.equals("S"), "Unknown relation: "
                + _rel);
    }

    private int _tid;

    private FileWriter _output;
    private SpoutOutputCollector _collector;

    private ConcurrentMap<UUID, Values> _pending;
    private Random _rand;
    private String _sourceName;
    private long _seq;

    private long _interval;
    private int _tuplesPerTime;
    private boolean _fluctuation;

    private int _intLowerR;
    private int _intRangeR;
    private int _doubleLowerR;
    private int _doubleRangeR;
    private int _charsLengthR;

    private int _intLowerS;
    private int _intRangeS;
    private int _doubleLowerS;
    private int _doubleRangeS;
    private int _charsLengthS;

    private long _profileReportInSeconds;
    private long _triggerReportInSeconds;
    private Stopwatch _stopwatch;

    private long _numTuplesEmitted;

    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        _tid = context.getThisTaskId();

        String outputDir = getString(conf.get("outputDir"));
        String prefix = "i" + _rel.toLowerCase() + _tid;
        _output = (new FileWriter(outputDir, prefix, "txt")).setFlushSize(10)
                .setPrintStream(System.out);

        _collector = collector;

        _pending = (new MapMaker()).makeMap();

        long seed = 43;
        if (!_rel.isEmpty()) {
            seed = System.nanoTime();
        }
        _rand = new Random(seed);

        _sourceName = "k-" + context.getThisTaskId();

        _seq = 0;

        long tuplesPerSecond;
        if (_rel.isEmpty()) {
            tuplesPerSecond = getLong(conf.get("tuplesPerSecond"));
        }
        else if (_rel.equals("R")) {
            tuplesPerSecond = getLong(conf.get("tuplesPerSecondR"));
        }
        else { // _rel.equals("S")
            tuplesPerSecond = getLong(conf.get("tuplesPerSecondS"));
        }
        convertToEmitRateSettings(tuplesPerSecond);

        _fluctuation = getBoolean(conf.get("fluctuation"));

        int intLower = getInt(conf.get("intLower"));
        int intUpper = getInt(conf.get("intUpper"));
        int doubleLower = getInt(conf.get("doubleLower"));
        int doubleUpper = getInt(conf.get("doubleUpper"));
        int charsLength = getInt(conf.get("charsLength"));

        _intLowerR = intLower;
        _intRangeR = intUpper - intLower;
        _doubleLowerR = doubleLower;
        _doubleRangeR = doubleUpper - doubleLower;
        _charsLengthR = charsLength;

        _intLowerS = intLower;
        _intRangeS = intUpper - intLower;
        _doubleLowerS = doubleLower;
        _doubleRangeS = doubleUpper - doubleLower;
        _charsLengthS = charsLength - 5;

        LOG.info("relation:" + (_rel.isEmpty() ? "R/S" : _rel) + ", source:"
                + _sourceName + ", interval(ms):" + _interval
                + ", tuples_per_time:" + _tuplesPerTime + ", fluctuation:"
                + _fluctuation);

        /* profiling */
        _numTuplesEmitted = 0;

        _profileReportInSeconds = PROFILE_REPORT_PERIOD_IN_SEC;
        _triggerReportInSeconds = _profileReportInSeconds;
        _stopwatch = Stopwatch.createStarted();
    }

    public void nextTuple() {
        for (int i = 0; i < _tuplesPerTime; ++i) {
            /* generate data */
            Values values = generateData();

            /* send data */
            UUID msgId = randomUUID();
            _pending.put(msgId, values);
            _collector.emit(values, msgId);
        }
        _numTuplesEmitted += _tuplesPerTime;

        if (isTimeToOutputProfile()) {
            _output.write(getProfile());
        }

        /* sleep for a while... */
        if (_fluctuation) {
            sleep(_rand.nextInt((int) _interval));
        }
        else {
            sleep(_interval);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SCHEMA));
    }

    @Override
    public void close() {
        _stopwatch.stop();

        _output.write(getProfile());
        _output.endOfFile();
    }

    @Override
    public void ack(Object msgId) {
        _pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        _collector.emit(_pending.get(msgId), msgId);
    }

    private Values generateData() {
        ++_seq;

        String rel = _rel;
        if (rel.isEmpty()) {
            rel = _rand.nextBoolean() ? "R" : "S";
        }
        Long ts = System.currentTimeMillis();
        String seq = _sourceName + ":" + _seq;

        List<Object> payload = null;
        if (rel.equals("R")) {
            payload = generatePayloadR();
        }
        else if (rel.equals("S")) {
            payload = generatePayloadS();
        }
        else {
            LOG.error("Unknown relation");
        }

        return new Values(rel, ts, seq, payload);
    }

    private List<Object> generatePayloadR() {
        List<Object> payload = newArrayList();

        payload.add(new Integer(_intLowerR + _rand.nextInt(_intRangeR)));
        payload.add(new Double(_doubleLowerR + _rand.nextDouble()
                * _doubleRangeR));
        payload.add(randomAlphanumeric(_charsLengthR));

        return payload;
    }

    private List<Object> generatePayloadS() {
        List<Object> payload = newArrayList();

        payload.add(new Integer(_intLowerS + _rand.nextInt(_intRangeS)));
        payload.add(new Double(_doubleLowerS + _rand.nextDouble()
                * _doubleRangeS));
        payload.add(randomAlphanumeric(_charsLengthS));
        payload.add(new Boolean(_rand.nextBoolean()));

        return payload;
    }

    private void convertToEmitRateSettings(long tuplesPerSecond) {
        checkArgument(tuplesPerSecond >= 1);

        if (tuplesPerSecond <= 5) { // emit once per second
            _interval = 1000;
        }
        else if (tuplesPerSecond <= 20) { // emit twice per second
            _interval = 500;
        }
        else if (tuplesPerSecond <= 100) { // emit 5 times per second
            _interval = 200;
        }
        else { // emit 10 times per second
            _interval = 100;
        }

        _tuplesPerTime = (int) (tuplesPerSecond * _interval / 1000);
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

        sb.append("[Input" + (_rel.isEmpty() ? "" : "-") + _rel + "-" + _tid);
        sb.append(" @ " + _stopwatch.elapsed(SECONDS) + " sec]");

        sb.append(" emitted=" + _numTuplesEmitted);

        return sb.toString();
    }
}