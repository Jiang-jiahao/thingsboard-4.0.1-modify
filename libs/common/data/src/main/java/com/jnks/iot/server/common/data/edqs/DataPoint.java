package com.jnks.iot.server.common.data.edqs;

import com.jnks.iot.server.common.data.kv.DataType;

public interface DataPoint extends Comparable<DataPoint> {

    String NOT_SUPPORTED = "Not supported!";

    long getTs();

    DataType getType();

    String getStr();

    long getLong();

    double getDouble();

    boolean getBool();

    String getJson();

    String valueToString();

}
