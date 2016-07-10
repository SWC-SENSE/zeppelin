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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.flink.FlinkInterpreter;
import org.apache.zeppelin.flink.ZeppelinContext;
import org.apache.zeppelin.flink.ui.utils.StreamManager;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * List sink.
 */
public class FilteredListSink extends ListSink  {
  private StreamManager<FilterControllMessage> streamManager;
  Logger logger = LoggerFactory.getLogger(FilteredListSink.class);

  public String filterTerm = "";


  public FilteredListSink(){
    super();
    streamManager = new StreamManager<FilterControllMessage>(this.parragraphid, TypeInformation.of(FilterControllMessage.class));
    logger.info("Start FilteredListSink");
    final ZeppelinContext z = FlinkInterpreter.z;

    z.angularBindParagraph("filter_input" , filterTerm, parragraphid);
    z.angularWatchParagraph("filter_input", parragraphid, new AngularObjectWatcher(z.getInterpreterContext()) {
      @Override
      public void watch(Object oldObject, Object newObject, InterpreterContext context) {
        logger.info("FilteredListSink: input changed", newObject);
        filterTerm = (String) newObject;
        FilterControllMessage  msg = new FilterControllMessage();
        msg.filterMessage = filterTerm;
        streamManager.addMessage(msg);
      }
    });

  }

  public SourceFunction<FilterControllMessage> getControlStream() {
    return streamManager.getStream();
  }

  @Override
  public String toString() {
    return "%angular \n " + this.getListView() +
            " \n  <input type='text' class='form-control' ng-model='filter_input'" +
            " ></input> \n %text ";
  }


}
