/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.mqtt.limits;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ipfilter.AbstractRemoteAddressFilter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.mqtt.MqttTransportContext;
import org.thingsboard.server.transport.mqtt.MqttTransportService;

import java.net.InetSocketAddress;


/**
 * IP 过滤器
 * <p>
 * 在连接建立早期（channelActive 之前）根据客户端的远程 IP 地址进行黑白名单检查。
 * 如果通过检查，将真实地址存入 Channel 的 attribute 中，供后续处理器使用；
 * 否则拒绝连接（底层自动关闭 Channel）。
 */
@Slf4j
public class IpFilter extends AbstractRemoteAddressFilter<InetSocketAddress> {

    /**
     * MQTT 传输上下文，包含配置信息和全局状态，用于执行 IP 地址检查。
     */
    private MqttTransportContext context;

    public IpFilter(MqttTransportContext context) {
        this.context = context;
    }

    /**
     * 决定是否接受来自远程地址的连接。
     * 此方法在 Channel 注册后、激活前被调用。
     *
     * @param ctx           ChannelHandler 上下文
     * @param remoteAddress 客户端的远程 IP 地址和端口
     * @return true 表示接受连接，false 表示拒绝连接（Channel 将被关闭）
     * @throws Exception 处理过程中可能抛出的异常
     */
    @Override
    protected boolean accept(ChannelHandlerContext ctx, InetSocketAddress remoteAddress) throws Exception {
        log.trace("[{}] Received msg: {}", ctx.channel().id(), remoteAddress);
        // 调用上下文的地址检查逻辑（基于配置的黑白名单，限流等）
        if(context.checkAddress(remoteAddress)){
            log.trace("[{}] Setting address: {}", ctx.channel().id(), remoteAddress);
            // 将验证通过的地址存入 Channel 的 attribute，后续处理器可通过 MqttTransportService.ADDRESS 获取
            ctx.channel().attr(MqttTransportService.ADDRESS).set(remoteAddress);
            return true;
        } else {
            // 地址不在白名单中，拒绝连接
            return false;
        }
    }
}
