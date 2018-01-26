package scalalg.me.libme.rec

import me.libme.kernel._c.json.JJSON
import me.libme.rec.receiver.model.{CellData, TrackData}
import me.libme.xstream.{Compositer, FlexTupe, Tupe}

import scalalg.me.libme.module.hbase.HBaseConnector

/**
  * Created by J on 2018/1/19.
  */
class HBasePersist(executor:HBaseConnector#HBaseExecutor, algorithm: Algorithm,tableMatch: TableMatch,columnFamilyMatch: ColumnFamilyMatch,countEval: CountEval) extends Compositer{


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

  override def complete(tupe: Tupe): Unit = {

  }



}
