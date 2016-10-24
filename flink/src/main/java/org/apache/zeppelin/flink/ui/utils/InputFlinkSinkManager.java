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
package org.apache.zeppelin.flink.ui.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.zeppelin.flink.ui.UIComponent;

import java.io.Serializable;

/**
 * Created by philipp on 8/10/16.
 */
public class InputFlinkSinkManager<Type> extends RichSinkFunction<Type> implements Serializable {

    transient private final UIComponent<Type, ?> uiComponent;

    public InputFlinkSinkManager(UIComponent<Type, ?> uiComponent) {
        this.uiComponent = uiComponent;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(Type type) throws Exception {
        uiComponent.invoke(type);
    }
}
