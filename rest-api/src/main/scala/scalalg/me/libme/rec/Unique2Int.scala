package scalalg.me.libme.rec

import me.libme.rec.fn.u2i.ReadOnlyU2I
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.{Compositer, Tupe, TupeContext}

/**
  * Created by J on 2018/1/26.
  */
class Unique2Int extends Compositer{

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


    val unique2IntMark:ReadOnlyU2I =ReadOnlyU2I.get();

    val userId=data.getUserItemRecord.getUserId
    val itemId=data.getUserItemRecord.getItemId

    val intUserId=unique2IntMark.toInt(userId)
    val intItemId=unique2IntMark.toInt(itemId)

    data.getUserItemRecord.setUserId(String.valueOf(intUserId))
    data.getUserItemRecord.setItemId(String.valueOf(intItemId))

    tupeContext.produce(data)

  }

  override def complete(tupe: Tupe, tupeContext: TupeContext): Unit = {

  }




}
