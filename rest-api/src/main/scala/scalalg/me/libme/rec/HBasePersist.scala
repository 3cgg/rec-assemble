package scalalg.me.libme.rec

import me.libme.kernel._c.json.JJSON
import me.libme.rec.receiver.model.{CellData, TrackData}
import me.libme.xstream.{Compositer, Tupe, TupeContext}

import scalalg.me.libme.module.hbase.HBaseConnector

/**
  * Created by J on 2018/1/19.
  */
class HBasePersist(executor:HBaseConnector#HBaseExecutor, algorithm: Algorithm,tableMatch: TableMatch,columnFamilyMatch: ColumnFamilyMatch,countEval: CountEval) extends Compositer{


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

    tupeContext.produce(data)

  }

  override def complete(tupe: Tupe, tupeContext: TupeContext): Unit = {

  }



}
