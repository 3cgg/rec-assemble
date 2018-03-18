package scalalg.me.libme.rec

import java.util

import me.libme.rec.fn.u2i.ReadOnlyU2I
import me.libme.rec.receiver.model.TrackData
import me.libme.xstream.EntryTupe.Entry
import me.libme.xstream.{Compositer, ConsumerMeta, Tupe}

/**
  * Created by J on 2018/1/26.
  */
class Unique2Int(consumerMeta: ConsumerMeta) extends Compositer(consumerMeta: ConsumerMeta){

  override def prepare(tupe: Tupe[_]): Unit ={
    super.prepare(tupe)
  }

  override def _finally(tupe: Tupe[_]): Unit = {

  }

  override def doConsume(tupe: Tupe[_]): Unit = {

    var data:TrackData=null
    val iterator:util.Iterator[_]=tupe.iterator()
    if(iterator.hasNext){
      data= classOf[TrackData].cast(
        classOf[Entry].cast(iterator.next()).getValue
        )
    }


    val unique2IntMark:ReadOnlyU2I =ReadOnlyU2I.get();

    val userId=data.getUserItemRecord.getUserId
    val itemId=data.getUserItemRecord.getItemId

    val intUserId=unique2IntMark.toInt(userId)
    val intItemId=unique2IntMark.toInt(itemId)

    data.getUserItemRecord.setUserId(String.valueOf(intUserId))
    data.getUserItemRecord.setItemId(String.valueOf(intItemId))


  }

  override def complete(tupe: Tupe[_]): Unit = {

  }




}
