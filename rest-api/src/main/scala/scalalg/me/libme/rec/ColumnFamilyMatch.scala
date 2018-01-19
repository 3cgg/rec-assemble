package scalalg.me.libme.rec

import me.libme.rec.receiver.vo.TrackData

/**
  * Created by J on 2018/1/19.
  */
trait ColumnFamilyMatch {

  def matches(trackData: TrackData):String

}
