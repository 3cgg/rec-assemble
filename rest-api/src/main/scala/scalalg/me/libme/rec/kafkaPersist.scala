package scalalg.me.libme.rec

import java.util

import me.libme.kernel._c.json.JJSON
import me.libme.module.kafka.SimpleProducer
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.{Compositer, ConsumerMeta, Tupe}

/**
  * Created by J on 2018/1/19.
  */
class kafkaPersist(producer :SimpleProducer, topicMatch: TopicMatch,consumerMeta: ConsumerMeta) extends Compositer(consumerMeta: ConsumerMeta){


  override def prepare(tupe: Tupe[_]): Unit ={
    super.prepare(tupe)
  }

  override def _finally(tupe: Tupe[_]): Unit = {

  }

  override def doConsume(tupe: Tupe[_]): Unit = {
    var data:TrackData=null
    val iterator:util.Iterator[_]=tupe.iterator()
    if(iterator.hasNext){
      data= classOf[TrackData].cast(iterator.next())
    }

    val topic=topicMatch.matches(data)
    val string=JJSON.get().formatJSONObject(data);
    producer.send(string,topic)

    produce(data)

  }

  override def complete(tupe: Tupe[_]): Unit = {

  }



}
