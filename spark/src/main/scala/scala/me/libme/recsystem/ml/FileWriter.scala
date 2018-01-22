package scala.me.libme.recsystem.ml
import java.io
import java.io.BufferedWriter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema


/**
  * Created by J on 2018/1/8.
  */
class FileWriter extends RowOutput[Unit]{

  /**
    * "D:\java_\spark\result_movielens_ratings.txt"
    */
  var filePath:String="D:\\java_\\spark\\result_movielens_ratings.txt"

  var count:Int=10


  def appendChar(c:String):Unit={

    var bw:Option[BufferedWriter]=None

    try{
      bw=Some(new BufferedWriter(new io.FileWriter(filePath,true)))
      bw.get.append(c)
    }catch {
      case e:Exception =>{
        throw new RuntimeException(e)
      }

    }finally {
      bw.foreach(f=>f.close())
    }
  }

  override def write(dataFrame: DataFrame): Unit = {

    // write schema / struct
    val header=new StringBuffer()

    val schema=dataFrame.schema
    schema.foreach(field=>{
      header.append(field.name+"::")
    })

    appendChar(header.toString+"\n")


    var index=0
    val max=3
    var values=""
    dataFrame.take(count).foreach(row=>{

      val t:String=""+row.getInt(0)
      values+="::"+t
      row.getSeq[GenericRowWithSchema](1).foreach(grs=>{
        for (i <- 0 until grs.length) {
          values+="::"+grs.get(i)
        }
        values+="\n"
      })

      values+="\n"
      index+=1
      if(index>max){
        //flush
//        JIOUtils.write(new File(filePath),values.getBytes("utf-8"))
        appendChar(values)
        values=""
      }
    })

    if(index<max&&index!=0){
      appendChar(values)
    }

    println("Write completely...")

  }


}
