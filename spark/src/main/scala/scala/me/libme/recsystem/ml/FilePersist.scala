package scala.me.libme.recsystem.ml

import java.io.BufferedWriter
import java.{io, util}

import me.libme.recsystem.ml.RatingPersist

import scala.collection.JavaConversions

/**
  * Created by J on 2018/1/9.
  */
class FilePersist extends RatingPersist{


  var filePath:String="D:\\java_\\spark\\result_movielens_ratings.txt"

  private def appendChar(c:String):Unit={

    var bw:Option[BufferedWriter]=None

    try{
      bw=Some(new BufferedWriter(new io.FileWriter(filePath,true)))
      bw.get.append(c).flush()
    }catch {
      case e:Exception =>{
        throw new RuntimeException(e)
      }

    }finally {
      bw.foreach(f=>f.close())
    }
  }

  override def persist(ratings: util.List[UserItemStruct]): Unit = {

    appendChar("userId::itemId::rating::timestamp")

    var index=0
    val max=3
    var values=""

    JavaConversions.asScalaBuffer(ratings).foreach(uis=>{
      uis.itemRatings.foreach(uir=>{
        values+="\n"+uis.userId+"::"+uir.itemId+"::"+uir.rating+"::"+uir.timestamp
        index+=1
        if(index%max==0){
          appendChar(values)
          values=""
        }

      })
    })

    if(index%max!=0){
      appendChar(values)
      values=""
    }

    println("===================================OK===============================")


  }

}
