/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.zeppelin.flink.FlinkInterpreter;
import org.apache.zeppelin.flink.ui.utils.InputFlinkSink;
import org.apache.zeppelin.flink.ui.utils.OutputFlinkStream;
import org.apache.zeppelin.flink.ui.utils.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by philipp on 8/9/16.
 */
public abstract class UIComponent<InputType,OutputType> implements Serializable {

    private final InputFlinkSink<InputType> inputSink;
    private OutputFlinkStream<OutputType> outputStream;

    protected final String paragraphId = FlinkInterpreter.z.getInterpreterContext().getParagraphId();
    protected Logger logger =  LoggerFactory.getLogger(this.getClass());
    protected final Scope scope = new Scope(paragraphId);

    protected void addMessageToOutputStream(OutputType item){
        outputStream.addMessage(item);
    }

    public UIComponent(Class<OutputType> clazz){
      String id = FlinkInterpreter.z.getInterpreterContext().getParagraphId();
      this.outputStream = new OutputFlinkStream<OutputType>(id + "_output", TypeInformation.of(clazz));
      this.inputSink = new InputFlinkSink<InputType>(this);
    }

    public abstract String getTremplate();

    @Override
    public String toString() {
        return "%angular \n" +getTremplate();
    }

    public RMQSource<OutputType> getOutputStreamStream(){
        return outputStream.getStream();
    }

    public RichSinkFunction<InputType> getSink(){
        return new RichSinkFunction<InputType>() {

            @Override
            public void invoke(InputType inputType) throws Exception {
                UIComponent.this.invoke(inputType);
            }
        };
    }

    public void invoke(InputType type) {
    }
}
