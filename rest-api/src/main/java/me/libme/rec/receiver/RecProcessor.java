package me.libme.rec.receiver;

import me.libme.kernel._c.util.CliParams;
import me.libme.module.kafka.ProducerConnector;
import me.libme.module.kafka.SimpleProducer;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.xstream.*;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scalalg.me.libme.module.hbase.HBaseConnector;
import scalalg.me.libme.rec.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

        WindowTopology.WindowBuilder windowBuilder=WindowTopology.builder().setName("Track Data Topology")
                .setCount(recProcessorBuilder.count)
                .setTime(recProcessorBuilder.time)
                .setSourcer(queueWindowSourcer);

        if(recProcessorBuilder.persistKafka) {
            TopicMatch topicMatch=recProcessorBuilder.topicMatch;
            kafkaPersist kafkaPersist = new kafkaPersist(simpleProducer, topicMatch);
            windowBuilder.addConsumer(kafkaPersist);
        }

        if(recProcessorBuilder.persistHbase){
            HBasePersist hBasePersist=new HBasePersist(hbaseExecutor, Plus$.MODULE$,DefaultConfig._TableMatch$.MODULE$,
                    DefaultConfig._ColumnFamilyMatch$.MODULE$,DefaultConfig._CountEval$.MODULE$);
            windowBuilder.addConsumer(hBasePersist);
        }

        for(OperationProvider operationProvider:recProcessorBuilder.operationProviders){
            windowBuilder.addOperation(operationProvider.provide(zookeeperExecutor,producerExecutor,hbaseExecutor));
        }

        windowBuilder.build().start();

    }


    public static class RecProcessorBuilder{

        private QueueHolder queueHolder;

        private List<OperationProvider> operationProviders=new ArrayList<>();

        private String[] args=new String[]{};

        private int count=1000 ;

        private int time=1*1000; //millisecond

        private TopicMatch topicMatch=trackData->"rec-system";

        boolean persistKafka=true;

        boolean persistHbase=true;

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
            operationProviders.add(consumerProider);
            return this;
        }

        public RecProcessorBuilder addFilterProider(FilterProider filterProider){
            operationProviders.add(filterProider);
            return this;
        }


        public RecProcessorBuilder setPersistKafka(boolean persistKafka) {
            this.persistKafka = persistKafka;
            return this;
        }

        public RecProcessorBuilder setPersistHbase(boolean persistHbase) {
            this.persistHbase = persistHbase;
            return this;
        }

        public RecProcessor build(){
            RecProcessor recProcessor=new RecProcessor();
            recProcessor.recProcessorBuilder=this;
            return recProcessor;
        }


    }

    public interface OperationProvider<T extends Operation>{

        T provide(ZooKeeperConnector.ZookeeperExecutor zookeeperExecutor,
                  ProducerConnector.ProducerExecutor producerExecutor,
                  HBaseConnector.HBaseExecutor hbaseExecutor);

    }

    public interface ConsumerProider extends OperationProvider<Consumer>{

    }

    public interface FilterProider extends OperationProvider<Filter>{

    }





}
