package scalalg.me.libme.rec
import me.libme.rec.receiver.vo.{EventType, TrackData}

/**
  * Created by J on 2018/1/19.
  */
object DefaultConfig {


  object _TableMatch extends TableMatch{
    override def matches(trackData: TrackData): String = "userItem"
  }

  object _ColumnFamilyMatch extends ColumnFamilyMatch{
    override def matches(trackData: TrackData): String = "item"
  }

  object _CountEval extends CountEval{
    override def eval(trackData: TrackData): Int = {

      val eventType=trackData.getEvent.getType

      var count:Int=0
      eventType match {

        case EventType.click =>{
          count=10
        }
        case EventType.browser =>{
          count=5
        }
      }
      return count

    }

  }


}
