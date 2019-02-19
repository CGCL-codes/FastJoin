package soj.biclique.core;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import soj.util.FileWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ContRandGrouping implements CustomStreamGrouping, Serializable {
    private List<Integer> _targetTasks;
    private HashFunction h = Hashing.murmur3_128(13);
    private FileWriter _output;
    private int _groupMembers;
    private int _groups;
    private String _rel;


    public ContRandGrouping(int groupMembers, String rel){
        _groupMembers = groupMembers;
        _rel = rel;
    }
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        _groups = targetTasks.size() / _groupMembers;
        if(targetTasks.size() % _groupMembers != 0){
            _groups++;
        }
        //_output = new FileWriter("/tmp/", "zsj_grouping", "txt").setFlushSize(10);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        String vRel = values.get(0).toString();
        String key = values.get(2).toString();
        int groupid = (int) (Math.abs(h.hashBytes(key.getBytes()).asLong()) % _groups);
        for(int i = groupid * _groupMembers; i < ( groupid + 1 ) * _groupMembers && i < _targetTasks.size(); ++i) {
            boltIds.add(_targetTasks.get(i));
        }
        //_output.writeImmediately(vRel);
        if(_rel.equals(vRel)){
            //randomly pick one
            Random rand = new Random();
            int targetId = boltIds.get(Math.abs(rand.nextInt() % boltIds.size()));
            boltIds = new ArrayList<Integer>();
            boltIds.add(targetId);
        }
        return boltIds;
    }
}