package scalalg.me.libme.rec

import me.libme.kernel._c.util.CliParams

/**
  * Created by J on 2018/1/19.
  */
object RecAssembleParam {

  val click_count="--rec.eval.count.click";

  val browser_count="--rec.eval.count.browser";

  val json_file="--rec.json.file";

  def clickCount(cliParams: CliParams):Int={

    if(cliParams.contains(click_count)){
      return cliParams.getInt(click_count)
    }

    return 0




  }



}

