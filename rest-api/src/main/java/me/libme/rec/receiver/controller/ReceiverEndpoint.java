package me.libme.rec.receiver.controller;

import me.libme.kernel._c.json.JJSON;
import me.libme.kernel._c.util.CliParams;
import me.libme.kernel._c.util.JIOUtils;
import me.libme.kernel._c.util.JStringUtils;
import me.libme.module.kafka.ProducerConnector;
import me.libme.module.kafka.SimpleProducer;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.rec.RecApplication;
import me.libme.rec.receiver.vo.Content;
import me.libme.rec.receiver.vo.Event;
import me.libme.rec.receiver.vo.TrackData;
import me.libme.rec.receiver.vo.UserItemRecord;
import me.libme.webboot.ResponseModel;
import me.libme.webseed.web.ClosureException;
import me.libme.xstream.QueueWindowSourcer;
import me.libme.xstream.WindowTopology;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scalalg.me.libme.module.hbase.HBaseConnector;
import scalalg.me.libme.rec.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by J on 2018/1/17.
 */
@Controller
@RequestMapping("/receiver")
@ClosureException
public class ReceiverEndpoint implements ApplicationListener<ContextRefreshedEvent> {


    private static ArrayBlockingQueue<TrackData> queue=new ArrayBlockingQueue<>(100000);


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        QueueWindowSourcer queueWindowSourcer=new QueueWindowSourcer(queue);


        Map<String,Object> config=JJSON.get().parse(JStringUtils.utf8(
                 JIOUtils.getBytes(Thread.currentThread().getContextClassLoader().getResourceAsStream("rec-assemble.json"))));

        CliParams cliParams=new CliParams(RecApplication.args);
        for(Map.Entry<String,Object> entry:config.entrySet()){
            if(!cliParams.contains(entry.getKey())){
                cliParams=cliParams.append(entry.getKey(), String.valueOf(Optional.of(entry.getValue()).orElseGet(()->"")));
            }
        }


        Buffer buffer= JavaConversions.asScalaBuffer(Arrays.asList(cliParams));

        //kafka
        ProducerConnector.ProducerExecutor producerExecutor=Util.producerExecutor(buffer);

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor zookeeperExecutor=Util.zookeeper(buffer);

        //hbase
        HBaseConnector.HBaseExecutor hbaseExecutor=Util.hbase(buffer);


        SimpleProducer simpleProducer=new SimpleProducer(producerExecutor);

        TopicMatch topicMatch=new TopicMatch() {
            @Override
            public String matches(TrackData trackData) {
                return "rec-system";
            }
        };

        kafkaPersist kafkaPersist=new kafkaPersist(simpleProducer,topicMatch);

        HBasePersist hBasePersist=new HBasePersist(hbaseExecutor, Sqrt$.MODULE$,DefaultConfig._TableMatch$.MODULE$,
                DefaultConfig._ColumnFamilyMatch$.MODULE$,DefaultConfig._CountEval$.MODULE$);



        WindowTopology.builder().setName("Track Data Topology")
                .setCount(888)
                .setSourcer(queueWindowSourcer)
                .addConsumer(kafkaPersist)
                .addConsumer(hBasePersist)
//                .addConsumer(tupe->{
//                    System.out.println("-----go end----:"+tupe);
//                })
                .build().start();


    }

    @ResponseBody
    @RequestMapping(path="/message",method= RequestMethod.POST)
    public ResponseModel message (Event event, UserItemRecord userItemRecord, Content content) throws Exception {
        // do something validation on the contentRecord.

        TrackData trackData=new TrackData();
        trackData.setEvent(event);
        trackData.setUserItemRecord(userItemRecord);
        trackData.setContent(content);

        queue.offer(trackData);

        return ResponseModel.newSuccess(true);
    }




}
