package scalalg.me.libme.rec
import me.libme.rec.receiver.model.{EventType, TrackData}

/**
  * Created by J on 2018/1/19.
  */
object DefaultConfig {


  object _TableMatch extends TableMatch{
    override def matches(trackData: TrackData): String =String.valueOf(RecConfig.get("--rec.table.name"))
  }

  object _ColumnFamilyMatch extends ColumnFamilyMatch{
    override def matches(trackData: TrackData): String =String.valueOf(RecConfig.get("--rec.table.column.family"))
  }

  object _CountEval extends CountEval{
    override def eval(trackData: TrackData): Double = {

      val eventType=trackData.getEvent.getType

      var count:Double=0
      eventType match {

        case EventType.click =>{
          count=String.valueOf(RecConfig.get("--rec.eval.count.click")).toDouble
        }
        case EventType.browser =>{
          count=String.valueOf(RecConfig.get("--rec.eval.count.browser")).toDouble
        }
      }
      return count

    }

  }


}
