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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 * @param <Type>
 */
public class OutputFlinkStream<Type> implements Serializable {

    private RMQConnectionConfig config;
    private RMQSource<Type> source;
    private RMQSink rmqSink;

    public OutputFlinkStream(final String id, TypeInformation<Type> info){

        config = new RMQConnectionConfig.Builder().setHost("localhost")
                .setPort(5672).setPassword("guest")
                .setUserName("guest").setVirtualHost("/").build();
        rmqSink = new RMQSink<Type>(config, id, new TypeSchema<Type>(info));
        try {
            rmqSink.open(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        source = new RMQSource<Type>(config, id, new TypeSchema<Type>(info)){
            @Override
            protected void setupQueue() throws IOException {
                channel.queueDeclare(id, false, false, false, null);
            }
        };
    }

    public void addMessage(Type mesage){
        rmqSink.invoke(mesage);
    }

    public RMQSource<Type> getStream(){
        return source;
    }


}