package com.jnks.iot.server.transport.coap.client;

import org.eclipse.californium.core.observe.ObserveRelation;
import org.eclipse.californium.core.server.resources.CoapExchange;
import com.jnks.iot.server.common.adaptor.AdaptorException;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.coap.CoapSessionMsgType;

import java.util.concurrent.atomic.AtomicInteger;

public interface CoapClientContext {

    boolean registerAttributeObservation(TbCoapClientState clientState, String token, CoapExchange exchange);

    boolean registerRpcObservation(TbCoapClientState clientState, String token, CoapExchange exchange);

    AtomicInteger getNotificationCounterByToken(String token);

    TbCoapClientState getOrCreateClient(CoapSessionMsgType type, ValidateDeviceCredentialsResponse deviceCredentials, DeviceProfile deviceProfile) throws AdaptorException;

    TransportProtos.SessionInfoProto getNewSyncSession(TbCoapClientState clientState);

    void deregisterAttributeObservation(TbCoapClientState clientState, String token, CoapExchange exchange);

    void deregisterRpcObservation(TbCoapClientState clientState, String token, CoapExchange exchange);

    void reportActivity();

    void registerObserveRelation(String token, ObserveRelation relation);

    void deregisterObserveRelation(String token);

    boolean awake(TbCoapClientState client);
}
