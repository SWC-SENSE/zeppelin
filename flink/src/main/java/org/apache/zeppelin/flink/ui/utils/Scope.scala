/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.zeppelin.flink.ui.utils

import org.apache.zeppelin.display.AngularObjectWatcher
import org.apache.zeppelin.flink.FlinkInterpreter
import org.apache.zeppelin.interpreter.InterpreterContext

/**
  * Simple scope, which is similar to the angularjs scope object.
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
