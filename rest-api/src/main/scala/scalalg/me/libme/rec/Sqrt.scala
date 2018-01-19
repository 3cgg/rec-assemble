package scalalg.me.libme.rec

/**
  * Created by J on 2018/1/19.
  */
object Sqrt extends Algorithm{
  override def cal(one: Double, two: Double): Double = {

    Math.sqrt(one+two)

  }
}
