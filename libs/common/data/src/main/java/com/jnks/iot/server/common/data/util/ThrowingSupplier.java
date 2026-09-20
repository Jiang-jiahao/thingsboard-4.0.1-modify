package com.jnks.iot.server.common.data.util;

import com.jnks.iot.server.common.data.exception.JnksIotException;

@FunctionalInterface
public interface ThrowingSupplier<T> {

    T get() throws JnksIotException;

}
