# FastJoin
FastJoin is secondary development based on BiStream(http://www.comp.nus.edu.sg/~linqian/bistream) which is designed based on the join-biclique model.
FastJoin will outperform BiStream when processing skew data, which is very common in real world. 

## Building FastJoin

FastJoin source code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running

    mvn clean package

## Running FastJoin

FastJoin is built on top of [Storm](https://storm.apache.org/). After deploying a Storm cluster, you can launch BiStream by submitting its jar to the cluster. Please refer to Storm documents for how to [set up a Storm cluster](https://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) and [run topologies on a Storm cluster](https://storm.apache.org/documentation/Running-topologies-on-a-production-cluster.html).
Running 

    storm jar fastjoin-1.0-jar-with-dependencies.jar soj.biclique.KafkaTopology -n 48 --size 30G -t 2.2 -pr 24 -ps 24
(Didi data have to be import before running)
