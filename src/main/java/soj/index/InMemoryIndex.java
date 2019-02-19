package soj.index;

import java.util.List;

public interface InMemoryIndex<Key extends Comparable<Key>, Value>
{
    public int size();

    public int height();

    public List<Value> get(Key key);

    public void put(Key key, Value value);

    public void remove(Key key);

    public void remove(Key key, Value value);
}