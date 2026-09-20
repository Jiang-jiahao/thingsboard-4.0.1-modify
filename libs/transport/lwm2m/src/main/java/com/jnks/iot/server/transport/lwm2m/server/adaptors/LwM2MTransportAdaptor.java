package com.jnks.iot.server.transport.lwm2m.server.adaptors;

import com.google.gson.JsonElement;
import com.jnks.iot.server.common.adaptor.AdaptorException;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.Collection;

public interface LwM2MTransportAdaptor {

    TransportProtos.PostTelemetryMsg convertToPostTelemetry(JsonElement jsonElement) throws AdaptorException;

    TransportProtos.PostAttributeMsg convertToPostAttributes(JsonElement jsonElement) throws AdaptorException;

    TransportProtos.GetAttributeRequestMsg convertToGetAttributes(Collection<String> clientKeys, Collection<String> sharedKeys) throws AdaptorException;
}
