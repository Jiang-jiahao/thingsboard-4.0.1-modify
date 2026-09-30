package com.jnks.iot.server.transport.udp.util;

import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.TransportUdpDataType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethods;
import com.jnks.iot.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;
import com.jnks.iot.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;

import java.nio.charset.StandardCharsets;

/**
 * 按**档案**把一条下行 RPC 编成要发出去的字节。
 * <p>
 * 从 {@code UdpDeviceSession#onToDeviceRpcRequest} 抽出来，供两处共用：
 * ① 有会话时（会话按自己的档案编码）；② 没有会话时由出站会话直接发（见
 * {@code com.jnks.iot.server.transport.udp.outbound}）。两条路的线上字节必须完全一致，
 * 否则设备对同一条 RPC 会有两种解析结果。
 */
public final class UdpRpcFrameEncoder {

    private UdpRpcFrameEncoder() {
    }

    public static ByteBuf encode(DeviceProfile profile, ToDeviceRpcRequestMsg rpcRequest) {
        TransportUdpDataType dataType = payloadDataTypeOf(profile);
        String params = rpcRequest.getParams();
        // 自定义 JSON：params 就是负载，原样发 UTF-8 字节。
        // 不走 bodyBytesForDataType —— 负载编码是原始字节/协议模板时它会把含 "hex" 键的 JSON 解码成裸字节、破坏内容。
        if (DeviceProfileRpcMethods.isCustomJsonDownlink(profile, rpcRequest.getMethodName())) {
            return Unpooled.wrappedBuffer(params == null ? new byte[0] : params.getBytes(StandardCharsets.UTF_8));
        }
        // 协议模板 / 原始字节：params.hex 已由 UI buildHex 组好，线上只发 decode 后的原始字节，不再包 RPC 信封 JSON。
        if ((dataType == TransportUdpDataType.RAW_BYTES || dataType == TransportUdpDataType.PROTOCOL_TEMPLATE)
                && UdpPayloadUtil.isHexTemplateRpcParams(params)) {
            return UdpPayloadUtil.encodeBusinessFrame(
                    dataType, UdpTransportFramingMode.NONE, fixedFrameLengthOf(profile), params);
        }
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "rpc");
        msg.addProperty("requestId", rpcRequest.getRequestId());
        msg.addProperty("name", rpcRequest.getMethodName());
        msg.addProperty("params", params);
        msg.addProperty("expirationTime", rpcRequest.getExpirationTime());
        msg.addProperty("oneway", rpcRequest.getOneway());
        return Unpooled.wrappedBuffer(UdpPayloadUtil.bodyBytesForDataType(dataType, msg.toString()));
    }

    public static TransportUdpDataType payloadDataTypeOf(DeviceProfile profile) {
        UdpDeviceProfileTransportConfiguration cfg = udpConfig(profile);
        if (cfg == null || cfg.getTransportUdpDataTypeConfiguration() == null) {
            return TransportUdpDataType.UTF8;
        }
        return cfg.getTransportUdpDataTypeConfiguration().getTransportUdpDataType();
    }

    public static int fixedFrameLengthOf(DeviceProfile profile) {
        UdpDeviceProfileTransportConfiguration cfg = udpConfig(profile);
        if (cfg == null || cfg.getUdpFixedFrameLength() == null) {
            return 0;
        }
        return cfg.getUdpFixedFrameLength();
    }

    private static UdpDeviceProfileTransportConfiguration udpConfig(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration cfg)) {
            return null;
        }
        return cfg;
    }
}
