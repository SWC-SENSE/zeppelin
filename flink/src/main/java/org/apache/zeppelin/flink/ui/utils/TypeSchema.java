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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.zeppelin.flink.ui.InputControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 *
 * @param <T>
 */
public class TypeSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {
    Logger logger = LoggerFactory.getLogger(InputControl.class);
    private TypeInformation<T> info;
    private boolean endOfStream = false;

    TypeSchema(TypeInformation<T> info){

        this.info = info;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {

        if (bytes == null || bytes.length == 0){

            return null;
        }
        logger.info("bytes" + bytes.toString());
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream is = new ObjectInputStream(in);
            T obj = (T) is.readObject();
            return obj;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            logger.info("Error deserialisation",e);
        }

        return null;
    }

    @Override
    public boolean isEndOfStream(T t) {
        return this.endOfStream;
    }

    @Override
    public TypeInformation<T> getProducedType() {

        return info;
    }

    @Override
    public byte[] serialize(T t) {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream so = null;
        try {
            so = new ObjectOutputStream(bo);
            so.writeObject(t);
            so.flush();

            return bo.toByteArray();
        } catch (IOException e) {
            logger.info("Error serialize",e);
            e.printStackTrace();
        }

        return new byte[0];
    }
}