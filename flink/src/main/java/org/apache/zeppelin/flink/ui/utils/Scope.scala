package org.apache.zeppelin.flink.ui.utils

import org.apache.zeppelin.display.AngularObjectWatcher
import org.apache.zeppelin.flink.FlinkInterpreter
import org.apache.zeppelin.interpreter.InterpreterContext

/**
  * Created by philipp on 8/10/16.
  */
class Scope(paragraphId: String) extends Serializable{

  def put[Type](name: String, item: Type): Unit ={
    FlinkInterpreter.z.angularBindParagraph(name,item,paragraphId)
  }


  def remove(name: String): Unit = {
    FlinkInterpreter.z.angularUnbindParagraph(name,paragraphId)
  }

  def watch[Type](name:String, callback: (Type,Type)=>Unit): Unit ={
    FlinkInterpreter.z.angularWatchParagraph(name, paragraphId, new AngularObjectWatcher(FlinkInterpreter.z.getInterpreterContext()) {
      override def watch(oldObject: scala.Any, newObject: scala.Any, context: InterpreterContext){
         callback(oldObject.asInstanceOf[Type],newObject.asInstanceOf[Type])
      }
    })
  }
}
