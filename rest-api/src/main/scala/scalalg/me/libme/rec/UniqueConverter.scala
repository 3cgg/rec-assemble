package scalalg.me.libme.rec

import me.libme.kernel._c.json.JJSON
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.{Compositer, FlexTupe, Tupe}

/**
  * Created by J on 2018/1/26.
  */
class UniqueConverter extends Compositer{


  var _flexTupe :FlexTupe = null

  var _markerTupe:FlexTupe = null

  var _exceptionTupe:FlexTupe = null

  override def prepare(tupe: Tupe): Unit ={
    super.prepare(tupe)
    _flexTupe=new FlexTupe
    _markerTupe=new FlexTupe
    _exceptionTupe=new FlexTupe

  }

  override def exceptionTupe(): FlexTupe = {
    return _exceptionTupe
  }

  override def markerTupe(): FlexTupe = {
    return _markerTupe
  }

  override def _finally(tupe: Tupe): Unit = {

  }

  override def flexTupe(): FlexTupe = {
    return _flexTupe
  }

  override def doConsume(tupe: Tupe): Unit = {

    var data:TrackData=null
    if(tupe.hasNext){
      data= classOf[TrackData].cast(tupe.next())
    }

    val topic=topicMatch.matches(data)
    val string=JJSON.get().formatJSONObject(data);
    producer.send(string,topic)

    produce(data)

  }

  override def complete(tupe: Tupe): Unit = {

  }




}
