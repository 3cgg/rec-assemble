package scalalg.me.libme.rec

import java.util

import me.libme.kernel._c.json.JJSON
import me.libme.rec.receiver.model.{CellData, TrackData}
import me.libme.xstream.{Compositer, ConsumerMeta, Tupe}

import scalalg.me.libme.module.hbase.HBaseConnector

/**
  * Created by J on 2018/1/19.
  */
class HBasePersist(executor:HBaseConnector#HBaseExecutor, algorithm: Algorithm,tableMatch: TableMatch,columnFamilyMatch: ColumnFamilyMatch,countEval: CountEval,consumerMeta: ConsumerMeta) extends Compositer(consumerMeta: ConsumerMeta){


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

    val row=data.getUserItemRecord.getUserId
    val column=data.getUserItemRecord.getItemId

    val tableName=tableMatch.matches(data)
    val columnFamily=columnFamilyMatch.matches(data)


    // get the original rating
    val oneRating:CellData=Option(executor.columnOperations.get(tableName,columnFamily,column,row)) match {
      case Some(v) =>JJSON.get().parse(v,classOf[CellData])
      case None => CellData._default()
    }
    val evalVal=countEval.eval(data)
    val finalVal=algorithm.cal(oneRating.getRating,oneRating.getRating+evalVal)
    oneRating.setRating(finalVal)

    //calculate the final original rating
    executor.columnOperations.insert(tableName,columnFamily,column,row,JJSON.get().format(oneRating))

    produce(data)

  }

  override def complete(tupe: Tupe[_]): Unit = {

  }



}
