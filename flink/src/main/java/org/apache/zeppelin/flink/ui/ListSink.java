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
package org.apache.zeppelin.flink.ui;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.zeppelin.flink.FlinkInterpreter;
import org.apache.zeppelin.flink.ZeppelinContext;

import java.util.LinkedList;

/**
 * List sink.
 */
public class ListSink extends RichSinkFunction<String> {


  protected final String parragraphid;
  private final int maxListSize;


  public ListSink() {
    this(10);
  }
  LinkedList<String> list = new LinkedList<String>();
  public ListSink(int maxListSize) {
    this.maxListSize = maxListSize;
    final ZeppelinContext z = FlinkInterpreter.z;
    parragraphid = z.getInterpreterContext().getParagraphId();
    FlinkInterpreter.z.angularBindParagraph("list", this.list, parragraphid);

  }




  public void addItemToList(String item){
    this.list.addFirst(item);
    if (this.list.size() > maxListSize){
      this.list.removeLast();
    }
    FlinkInterpreter.z.angularBindParagraph("list", this.list, parragraphid);
  }

  @Override
  public void invoke(String value) throws Exception {
    addItemToList(value);
  }

  public String getListView(){
    return  "<div ng-repeat='i in list  track by $index'>{{i}}</div>";
  }

  @Override
  public String toString() {
    return "%angular \n " + getListView() + "\n";
  }
}
