package com.jnks.iot.server.common.data.id;

import java.io.Serializable;

public interface HasId<I extends HasUUID> extends Serializable {

    I getId();

}
