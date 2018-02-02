package scalalg.me.libme.rec

import me.libme.kernel._c.json.JJSON
import me.libme.module.kafka.SimpleProducer
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.{Compositer, Tupe, TupeContext}

/**
  * Created by J on 2018/1/19.
  */
class kafkaPersist(producer :SimpleProducer, topicMatch: TopicMatch) extends Compositer{

  override def prepare(tupe: Tupe): Unit ={
    super.prepare(tupe)
  }

  override def _finally(tupe: Tupe, tupeContext: TupeContext): Unit = {

  }

  override def doConsume(tupe: Tupe, tupeContext: TupeContext): Unit = {

    var data:TrackData=null
    if(tupe.hasNext){
      data= classOf[TrackData].cast(tupe.next())
    }

    val topic=topicMatch.matches(data)
    val string=JJSON.get().formatJSONObject(data);
    producer.send(string,topic)

    tupeContext.produce(data)

  }

  override def complete(tupe: Tupe, tupeContext: TupeContext): Unit = {

  }



}
