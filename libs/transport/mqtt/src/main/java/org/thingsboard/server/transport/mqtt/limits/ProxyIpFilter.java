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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.mqtt.MqttTransportContext;
import org.thingsboard.server.transport.mqtt.MqttTransportService;

import java.net.InetSocketAddress;

/**
 * 代理 IP 过滤器，用于处理通过 HAProxy 协议传递的客户端真实 IP。
 * <p>
 * 当 MQTT 服务部署在代理（如 HAProxy、Nginx）之后时，代理会发送 PROXY 协议消息，
 * 其中包含客户端的原始 IP 和端口。本处理器负责解析该消息，提取真实地址并进行黑白名单检查。
 * 检查通过后将地址存入 Channel attribute，并从 pipeline 中移除自身，后续消息直接传递给 MQTT 处理器。
 */
@Slf4j
public class ProxyIpFilter extends ChannelInboundHandlerAdapter {

    /**
     * MQTT 传输上下文，包含配置信息和全局状态，用于执行 IP 地址检查。
     */
    private MqttTransportContext context;

    public ProxyIpFilter(MqttTransportContext context) {
        this.context = context;
    }

    /**
     * 读取通道中的消息。期望第一个消息是 HAProxyMessage，解析出真实客户端地址。
     *
     * @param ctx ChannelHandler 上下文
     * @param msg 接收到的消息对象
     * @throws Exception 处理异常
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.trace("[{}] Received msg: {}", ctx.channel().id(), msg);
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage proxyMsg = (HAProxyMessage) msg;
            // 确保源地址和端口有效（忽略健康检查等无地址消息）
            if (proxyMsg.sourceAddress() != null && proxyMsg.sourcePort() > 0) {
                InetSocketAddress address = new InetSocketAddress(proxyMsg.sourceAddress(), proxyMsg.sourcePort());
                // 黑白名单检查
                if (!context.checkAddress(address)) {
                    // 检查不通过，关闭通道
                    closeChannel(ctx);
                } else {
                    log.trace("[{}] Setting address: {}", ctx.channel().id(), address);
                    // 将真实地址存入 Channel attribute，供后续处理器使用
                    ctx.channel().attr(MqttTransportService.ADDRESS).set(address);
                    // We no longer need this channel in the pipeline. Similar to HAProxyMessageDecoder
                    // HAProxy 消息已处理完毕，从 pipeline 中移除本处理器
                    // 后续消息（真正的 MQTT 协议数据）将直接传递给下一个处理器
                    ctx.pipeline().remove(this);
                }
            } else {
                log.trace("Received local health-check connection message: {}", proxyMsg);
                // 收到本地健康检查等无地址消息，直接关闭通道
                closeChannel(ctx);
            }
        }
    }

    /**
     * 关闭通道并清理 pipeline 中所有剩余的处理器。
     * 确保资源正确释放。
     *
     * @param ctx ChannelHandler 上下文
     */
    private void closeChannel(ChannelHandlerContext ctx) {
        // 依次同步移除（因为 ctx.close()是异步的）当前处理器之后的所有处理器，并调用它们的 channelUnregistered 方法
        while (ctx.pipeline().last() != this) {
            ChannelHandler handler = ctx.pipeline().removeLast();
            if (handler instanceof ChannelInboundHandlerAdapter) {
                try {
                    ((ChannelInboundHandlerAdapter) handler).channelUnregistered(ctx);
                } catch (Exception e) {
                    log.error("Failed to unregister channel: [{}]", ctx, e);
                }
            }

        }
        // 移除自身
        ctx.pipeline().remove(this);
        // 关闭通道
        ctx.close();
    }
}
