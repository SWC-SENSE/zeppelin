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
