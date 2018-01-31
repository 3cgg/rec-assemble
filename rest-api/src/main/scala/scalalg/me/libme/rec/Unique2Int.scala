package scalalg.me.libme.rec

import me.libme.rec.cluster.PathListenerClientFactory
import me.libme.rec.cluster._trait.Unique2IntMark
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.{Compositer, FlexTupe, Tupe}

/**
  * Created by J on 2018/1/26.
  */
class Unique2Int extends Compositer{


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


    val unique2IntMark:Unique2IntMark =PathListenerClientFactory.factory(classOf[Unique2IntMark],Unique2IntMark.PATH)

    val userId=data.getUserItemRecord.getUserId
    val itemId=data.getUserItemRecord.getItemId

    val intUserId=unique2IntMark.unique(userId,null)
    val intItemId=unique2IntMark.unique(itemId,null)

    data.getUserItemRecord.setUserId(String.valueOf(intUserId))
    data.getUserItemRecord.setItemId(String.valueOf(intItemId))

    produce(data)

  }

  override def complete(tupe: Tupe): Unit = {

  }




}