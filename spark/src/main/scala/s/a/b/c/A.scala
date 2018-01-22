package s.a.b.c

import a.b.c.D

/**
  * Created by J on 2017/12/29.
  */
object A {


  def main(args: Array[String]): Unit = {

    println("Hello World")


  }

  def imA():Unit={
    println("IamA")

    var d=new D()
    d.iamD()

  }

}

class B(name:String,age:Int){


  def showName():String={

    A.imA()
    name+age
  }


}
