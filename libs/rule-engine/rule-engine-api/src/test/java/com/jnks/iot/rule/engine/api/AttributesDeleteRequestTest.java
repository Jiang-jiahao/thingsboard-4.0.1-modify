package com.jnks.iot.rule.engine.api;

import org.junit.jupiter.api.Test;
import com.jnks.iot.common.util.NoOpFutureCallback;

import static org.assertj.core.api.Assertions.assertThat;

class AttributesDeleteRequestTest {

    @Test
    void testDefaultCallbackIsNoOp() {
        var request = AttributesDeleteRequest.builder().build();

        assertThat(request.getCallback()).isEqualTo(NoOpFutureCallback.instance());
    }

    @Test
    void testNullCallbackIsNoOp() {
        var request = AttributesDeleteRequest.builder().callback(null).build();

        assertThat(request.getCallback()).isEqualTo(NoOpFutureCallback.instance());
    }

}
