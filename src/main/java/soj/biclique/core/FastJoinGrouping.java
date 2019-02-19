package soj.biclique.core;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.hash.*;
import soj.util.FileWriter;
import soj.util.Stopwatch;
public class FastJoinGrouping implements CustomStreamGrouping, Serializable {
    private List<Integer> _targetTasks;
    private HashFunction h = Hashing.murmur3_128(13);
    private FileWriter _output;
    private long _lastOutputTime;
    private Stopwatch _stopwatch;
    private long _hashTimes;
    private long _routerTimes;
    public FastJoinGrouping(){
        _hashTimes = 0;
        _routerTimes = 0;
    }
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        //_output = new FileWriter("/tmp/", "zsj_grouping", "txt").setFlushSize(10);//..setPrintStream(System.out);
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if(values.size() > 0){
            String key = values.get(2).toString();
            Integer target = (Integer)values.get(4);
            //hash
            if(target == -1){
                int idx = (int) (Math.abs(h.hashBytes(key.getBytes()).asLong()) % this._targetTasks.size());
                boltIds.add(_targetTasks.get(idx));
                _hashTimes++;
            }
            else{
                boltIds.add(target);
                _routerTimes++;
            }
        }
//        long _currTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);;
//        if(_currTime > _lastOutputTime + 5000){
//           _output.writeImmediately("Hash times: " + _hashTimes + " router times: " + _routerTimes);
//        }
        return boltIds;
    }
}