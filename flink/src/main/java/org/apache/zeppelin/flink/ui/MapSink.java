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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.flink.FlinkInterpreter;
import org.apache.zeppelin.flink.ZeppelinContext;
import org.apache.zeppelin.flink.ui.utils.OutputFlinkStream;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * List sink.
 */
public class MapSink extends RichSinkFunction<MarkerMessages> {


  public final String parragraphid;
  public Map<Integer, MarkerMessages> markerMap = new HashMap<>();
  public MapCenter center = new MapCenter();
  private OutputFlinkStream<MapCenter> streamManager;
  Logger logger = LoggerFactory.getLogger(InputControl.class);
  public MapSink() {
    final ZeppelinContext z = FlinkInterpreter.z;
    parragraphid = z.getInterpreterContext().getParagraphId();
    streamManager = new OutputFlinkStream<>(parragraphid, TypeInformation.of(MapCenter.class));
    FlinkInterpreter.z.angularBindParagraph("leaflet_markers", null, parragraphid);
    FlinkInterpreter.z.angularBindParagraph("center", center, parragraphid);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("MapSink: register");
        FlinkInterpreter.z.angularWatchParagraph("center", parragraphid, new AngularObjectWatcher(FlinkInterpreter.z.getInterpreterContext()) {
          @Override
          public void watch(Object oldObject, Object newObject, InterpreterContext context) {
            logger.info("MapSink: input changed", newObject);
            center = (MapCenter) newObject;

            streamManager.addMessage(center);
          }
        });
      }
    }).start();
    FlinkInterpreter.z.angularBindParagraph("center_object", center, parragraphid);


  }

  public void addItemToList(MarkerMessages item){
    markerMap.put(markerMap.size(), item);
    FlinkInterpreter.z.angularBindParagraph("leaflet_markers", this.markerMap, parragraphid);
  }

  @Override
  public void invoke(MarkerMessages value) throws Exception {
    addItemToList(value);
  }

  public String getListView(){
    FlinkInterpreter.z.angularBindParagraph("leaflet_markers", null, parragraphid);

    return
            "<leaflet style='height:300px' center=\"center\" markers=\"leaflet_markers\"></leaflet> \n {{center}}";
  }

  public SourceFunction<MapCenter> getControlStream() {
    return streamManager.getStream();
  }

  @Override
  public String toString() {
    return "%angular \n " + getListView() + "\n";
  }


}
