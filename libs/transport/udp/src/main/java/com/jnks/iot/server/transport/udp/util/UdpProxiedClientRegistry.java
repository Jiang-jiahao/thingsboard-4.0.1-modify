package com.jnks.iot.server.transport.udp.util;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * nginx 的 UDP 代理（{@code proxy_protocol on}）**只在会话首包**上带 PROXY 头，同一会话后续数据报没有头。
 * <p>
 * 这里记住「代理侧对端（nginxIP:nginxPort —— nginx 每个会话用一个独立源端口）→ 真实客户端地址」，
 * 供同会话后续无头数据报沿用，这样：
 * <ul>
 *   <li>会话键（本地端口, 源 IP, 源端口）在整个会话内保持一致，不会因为头只来一次而分裂成两个会话；</li>
 *   <li>NONE 鉴权的 sourceHost 判定、以及下行回包地址都按真实设备走。</li>
 * </ul>
 * 条目在 TTL 内没有流量就淘汰（nginx 侧 proxy_timeout 结束后该会话不会再发包）。
 */
public final class UdpProxiedClientRegistry {

    private static final long TTL_MS = 600_000L;
    private static final int PRUNE_THRESHOLD = 1024;

    private final Map<InetSocketAddress, ClientRef> byProxyPeer = new ConcurrentHashMap<>();

    private record ClientRef(InetSocketAddress realClient, long lastSeenMs) {
    }

    /** 记住（或刷新）某个代理侧对端对应的真实客户端地址。 */
    public void remember(InetSocketAddress proxyPeer, InetSocketAddress realClient) {
        if (proxyPeer == null || realClient == null) {
            return;
        }
        byProxyPeer.put(proxyPeer, new ClientRef(realClient, System.currentTimeMillis()));
        if (byProxyPeer.size() > PRUNE_THRESHOLD) {
            prune(System.currentTimeMillis());
        }
    }

    /**
     * 该代理侧对端已知的真实客户端地址；未知或超过 TTL 返回 {@code null}（调用方按普通对端地址处理）。
     */
    public InetSocketAddress realClientFor(InetSocketAddress proxyPeer) {
        if (proxyPeer == null) {
            return null;
        }
        ClientRef ref = byProxyPeer.get(proxyPeer);
        if (ref == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        if (now - ref.lastSeenMs() > TTL_MS) {
            byProxyPeer.remove(proxyPeer, ref);
            return null;
        }
        byProxyPeer.put(proxyPeer, new ClientRef(ref.realClient(), now));   // 续期：长会话不因 TTL 掉线
        return ref.realClient();
    }

    private void prune(long now) {
        byProxyPeer.entrySet().removeIf(e -> now - e.getValue().lastSeenMs() > TTL_MS);
    }
}
