package me.libme.rec.receiver;

import me.libme.module.kafka.ProducerConnector;
import me.libme.module.kafka.SimpleProducer;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.xstream.*;
import scalalg.me.libme.module.hbase.HBaseConnector;
import scalalg.me.libme.rec.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

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

        RecRuntime recRuntime=RecRuntime.builder().args(recProcessorBuilder.args).getOrCreate();


        //kafka
        ProducerConnector.ProducerExecutor producerExecutor= recRuntime.producerExecutor().get();

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor zookeeperExecutor=recRuntime.zookeeperExecutor().get();

        //hbase
        HBaseConnector.HBaseExecutor hbaseExecutor=recRuntime.hbaseExecutor().get();


        SimpleProducer simpleProducer=new SimpleProducer(producerExecutor);

        WindowTopology.WindowBuilder windowBuilder=WindowTopology.builder().setName("Track Data Topology")
                .setCount(recProcessorBuilder.count)
                .setTime(recProcessorBuilder.time)
                .windowExecutor(recProcessorBuilder.windowExecutor)
                .executor(recProcessorBuilder.executor)
                .setSourcer(queueWindowSourcer);

        if(recProcessorBuilder.persistKafka) {
            TopicMatch topicMatch=recProcessorBuilder.topicMatch;
            kafkaPersist kafkaPersist = new kafkaPersist(simpleProducer, topicMatch);
            windowBuilder.addConsumer(kafkaPersist);
        }

        if(recProcessorBuilder.persistHbase){
            windowBuilder.addConsumer(new Unique2Int());
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


        private ScheduledExecutorService windowExecutor;

        private ExecutorService executor;

        /**
         * !important / micro-batch
         * @param windowExecutor
         * @return
         */
        public RecProcessorBuilder windowExecutor(ScheduledExecutorService windowExecutor) {
            this.windowExecutor = windowExecutor;
            return this;
        }

        /**
         * !important / executing thread pool
         * @param executor
         * @return
         */
        public RecProcessorBuilder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }


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
