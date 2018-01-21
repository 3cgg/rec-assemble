package me.libme.rec.receiver;

import me.libme.kernel._c.json.JJSON;
import me.libme.kernel._c.util.CliParams;
import me.libme.kernel._c.util.JIOUtils;
import me.libme.kernel._c.util.JStringUtils;
import me.libme.module.kafka.ProducerConnector;
import me.libme.module.kafka.SimpleProducer;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.xstream.Consumer;
import me.libme.xstream.QueueWindowSourcer;
import me.libme.xstream.WindowTopology;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scalalg.me.libme.module.hbase.HBaseConnector;
import scalalg.me.libme.rec.*;

import java.util.*;

/**
 * Created by J on 2018/1/20.
 */
public class RecProcessor {


    private RecProcessorBuilder recProcessorBuilder;

    private RecProcessor(){}

    public static RecProcessorBuilder builder(){
        return new RecProcessorBuilder();
    }

    public void start(){

        QueueWindowSourcer queueWindowSourcer=new QueueWindowSourcer(recProcessorBuilder.queueHolder.queue());


        Map<String,Object> config= RecConfig$.MODULE$.backend();

        CliParams cliParams=new CliParams(recProcessorBuilder.args);
        for(Map.Entry<String,Object> entry:config.entrySet()){
            if(!cliParams.contains(entry.getKey())){
                cliParams=cliParams.append(entry.getKey(), entry.getValue());
            }
        }


        Buffer buffer= JavaConversions.asScalaBuffer(Arrays.asList(cliParams));

        //kafka
        ProducerConnector.ProducerExecutor producerExecutor= Util.producerExecutor(buffer);

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor zookeeperExecutor=Util.zookeeper(buffer);

        //hbase
        HBaseConnector.HBaseExecutor hbaseExecutor=Util.hbase(buffer);


        SimpleProducer simpleProducer=new SimpleProducer(producerExecutor);

        TopicMatch topicMatch=recProcessorBuilder.topicMatch;

        kafkaPersist kafkaPersist=new kafkaPersist(simpleProducer,topicMatch);

        HBasePersist hBasePersist=new HBasePersist(hbaseExecutor, Plus$.MODULE$,DefaultConfig._TableMatch$.MODULE$,
                DefaultConfig._ColumnFamilyMatch$.MODULE$,DefaultConfig._CountEval$.MODULE$);


        WindowTopology.WindowBuilder windowBuilder=WindowTopology.builder().setName("Track Data Topology")
                .setCount(recProcessorBuilder.count)
                .setTime(recProcessorBuilder.time)
                .setSourcer(queueWindowSourcer)
                .addConsumer(kafkaPersist)
                .addConsumer(hBasePersist);
        for(ConsumerProider consumerProider:recProcessorBuilder.consumerProiders){
            windowBuilder.addConsumer(consumerProider.provide(zookeeperExecutor,producerExecutor,hbaseExecutor));
        }

        windowBuilder.build().start();

    }


    public static class RecProcessorBuilder{

        private QueueHolder queueHolder;

        private List<ConsumerProider> consumerProiders=new ArrayList<>();

        private String[] args=new String[]{};

        private int count=1000 ;

        private int time=1*1000; //millisecond

        private TopicMatch topicMatch=trackData->"rec-system";

        public RecProcessorBuilder setQueueHolder(QueueHolder queueHolder) {
            this.queueHolder = queueHolder;
            return this;
        }

        public RecProcessorBuilder setTopicMatch(TopicMatch topicMatch) {
            this.topicMatch = topicMatch;
            return this;
        }

        public RecProcessorBuilder setCount(int count) {
            this.count = count;
            return this;
        }

        public RecProcessorBuilder setTime(int time) {
            this.time = time;
            return this;
        }

        public RecProcessorBuilder setArgs(String[] args) {
            this.args = args;
            return this;
        }

        public RecProcessorBuilder addConsumerProider(ConsumerProider consumerProider){
            consumerProiders.add(consumerProider);
            return this;
        }

        public RecProcessor build(){
            RecProcessor recProcessor=new RecProcessor();
            recProcessor.recProcessorBuilder=this;
            return recProcessor;
        }


    }

    public interface ConsumerProider{

        Consumer provide(ZooKeeperConnector.ZookeeperExecutor zookeeperExecutor,
                         ProducerConnector.ProducerExecutor producerExecutor,
                         HBaseConnector.HBaseExecutor hbaseExecutor);

    }





}
