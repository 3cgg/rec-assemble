package scalalg.me.libme.rec

import me.libme.rec.receiver.model.TrackData

/**
  * Created by J on 2018/1/19.
  */
trait CountEval {

  def eval(trackData: TrackData): Int

}
