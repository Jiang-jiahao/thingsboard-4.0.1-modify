package com.jnks.iot.server.queue.discovery;

import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.List;

public interface DiscoveryService {

    /**
     * 获取其他服务实例信息
     * @return
     */
    List<TransportProtos.ServiceInfo> getOtherServers();

    /**
     * 判断是否为单体部署模式
     * @return
     */
    boolean isMonolith();

}
