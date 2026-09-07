/**
 * Copyright © 2016-2025 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.udp.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;

import java.util.concurrent.TimeUnit;

/**
 * 按设备/服务端配置安装或替换 UDP 分帧解码器（与 StringDecoder 解耦，下游统一为 {@link io.netty.buffer.ByteBuf}）。
 */
@Slf4j
public final class UdpPipelineBuilder {
    public static final String FRAMING_HANDLER_NAME = "tcpFraming";
    public static final String INBOUND_HANDLER_NAME = "tcpInbound";
    public static final String READ_IDLE_HANDLER_NAME = "tcpReadIdle";

    private UdpPipelineBuilder() {
    }

    public static ChannelHandler createFramingHandler(UdpTransportFramingMode mode, int maxFrameLength, int fixedFrameLength) {
        switch (mode) {
            case NONE:
                return null;
            case LINE:
                return new LineBasedFrameDecoder(maxFrameLength, true, true);
            case LENGTH_PREFIX_4:
                return new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4);
            case LENGTH_PREFIX_2:
                return new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 2, 0, 2);
            case FIXED_LENGTH:
                if (fixedFrameLength <= 0) {
                    throw new IllegalArgumentException("udpFixedFrameLength must be > 0 for FIXED_LENGTH framing");
                }
                return new FixedLengthFrameDecoder(fixedFrameLength);
            default:
                return new LineBasedFrameDecoder(maxFrameLength, true, true);
        }
    }

    /**
     * 首次安装：pipeline 中尚不存在 {@link #FRAMING_HANDLER_NAME}。
     */
    public static void addFramingFirst(ChannelPipeline pipeline, UdpTransportFramingMode mode, int maxFrameLength, int fixedFrameLength) {
        ChannelHandler handler = createFramingHandler(mode, maxFrameLength, fixedFrameLength);
        if (handler != null) {
            pipeline.addLast(FRAMING_HANDLER_NAME, handler);
        }
    }

    /**
     * 鉴权成功后，若与首帧分帧不一致则替换解码器（需在 eventLoop 线程调用）。
     */
    public static void replaceFramingIfNeeded(ChannelPipeline pipeline,
                                              UdpTransportFramingMode authMode,
                                              int authFixedLength,
                                              UdpTransportFramingMode profileMode,
                                              int profileFixedLength,
                                              int maxFrameLength) {
        if (framingEquals(authMode, authFixedLength, profileMode, profileFixedLength)) {
            return;
        }
        if (profileMode == UdpTransportFramingMode.FIXED_LENGTH && profileFixedLength <= 0) {
            log.warn("Profile uses FIXED_LENGTH but udpFixedFrameLength is unset; keeping auth framing");
            return;
        }
        log.debug("Replacing TCP framing: {} -> {}", authMode, profileMode);
        int fixed = profileMode == UdpTransportFramingMode.FIXED_LENGTH ? profileFixedLength : authFixedLength;
        ChannelHandler newHandler = createFramingHandler(profileMode, maxFrameLength, fixed);
        ChannelHandler current = pipeline.get(FRAMING_HANDLER_NAME);
        if (newHandler == null) {
            if (current != null) {
                pipeline.remove(FRAMING_HANDLER_NAME);
            }
            return;
        }
        if (current != null) {
            pipeline.replace(FRAMING_HANDLER_NAME, FRAMING_HANDLER_NAME, newHandler);
        } else {
            pipeline.addLast(FRAMING_HANDLER_NAME, newHandler);
        }
    }

    private static boolean framingEquals(UdpTransportFramingMode a, int aFixed, UdpTransportFramingMode b, int bFixed) {
        if (a != b) {
            return false;
        }
        return a != UdpTransportFramingMode.FIXED_LENGTH || aFixed == bFixed;
    }

    /**
     * 在 pipeline 最前安装读空闲检测；秒数 &lt;= 0 时移除已有 handler。
     */
    public static void installReadIdleHandlerFirst(ChannelPipeline pipeline, int readIdleSeconds) {
        if (readIdleSeconds <= 0) {
            ChannelHandler existing = pipeline.get(READ_IDLE_HANDLER_NAME);
            if (existing != null) {
                pipeline.remove(READ_IDLE_HANDLER_NAME);
            }
            return;
        }
        ChannelHandler idle = new IdleStateHandler(readIdleSeconds, 0, 0, TimeUnit.SECONDS);
        if (pipeline.get(READ_IDLE_HANDLER_NAME) != null) {
            pipeline.replace(READ_IDLE_HANDLER_NAME, READ_IDLE_HANDLER_NAME, idle);
        } else {
            pipeline.addFirst(READ_IDLE_HANDLER_NAME, idle);
        }
    }
}