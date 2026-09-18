package org.thingsboard.server.common.data.device.data;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Schema
@Data
public class DeviceData implements Serializable {

    private static final long serialVersionUID = -3771567735290681274L;

    @Schema(description = "Device configuration for device profile type. DEFAULT is only supported value for now")
    private DeviceConfiguration configuration;
    @Schema(description = "Device transport configuration used to connect the device")
    private DeviceTransportConfiguration transportConfiguration;

    /**
     * @deprecated 旧版全局固定参数；请使用 {@link #rpcParamDefaultsByMethod}。无按方法配置时部分客户端仍可能回退读取本字段。
     */
    @Schema(description = "Legacy per-device RPC defaults (all methods)")
    private Map<String, Object> rpcParamDefaults;

    /**
     * 按平台 RPC 方法 id 分组的固定参数：methodId →（平台参数名 → 值）。
     * 同一字段名在不同方法下可配置不同固定值。
     */
    @Schema(description = "Per-device fixed RPC defaults keyed by profile RPC method id")
    private Map<String, Map<String, Object>> rpcParamDefaultsByMethod;

    /**
     * 本设备要自动调用的档案 RPC 方法（开关、间隔、可选 params）。
     * 方法定义仍在设备档案；定时属于设备。
     */
    @Schema(description = "Per-device scheduled RPC invocations referencing profile method ids")
    private List<DeviceScheduledRpc> scheduledRpcs;

}
