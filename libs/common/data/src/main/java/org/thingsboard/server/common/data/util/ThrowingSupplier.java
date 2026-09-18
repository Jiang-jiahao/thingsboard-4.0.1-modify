package org.thingsboard.server.common.data.util;

import org.thingsboard.server.common.data.exception.ThingsboardException;

@FunctionalInterface
public interface ThrowingSupplier<T> {

    T get() throws ThingsboardException;

}
