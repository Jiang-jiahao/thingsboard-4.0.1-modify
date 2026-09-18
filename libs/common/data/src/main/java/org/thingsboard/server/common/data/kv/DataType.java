package org.thingsboard.server.common.data.kv;

import lombok.Getter;

public enum DataType {

    BOOLEAN(0),
    LONG(1),
    DOUBLE(2),
    STRING(3),
    JSON(4);

    @Getter
    private final int protoNumber; // Corresponds to KeyValueType

    DataType(int protoNumber) {
        this.protoNumber = protoNumber;
    }

}
