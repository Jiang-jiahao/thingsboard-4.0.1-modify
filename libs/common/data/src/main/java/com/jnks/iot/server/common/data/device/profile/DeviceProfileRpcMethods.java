package com.jnks.iot.server.common.data.device.profile;

import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.StringUtils;

import java.util.List;

/**
 * 档案 RPC 方法目录查询。TCP / UDP 传输层靠它在链路上判别绑定类型
 * —— {@code params} 到传输层时已是不透明字符串，没有别的判别位。
 */
public final class DeviceProfileRpcMethods {

    private DeviceProfileRpcMethods() {
    }

    /**
     * 按平台侧方法名（{@link DeviceProfileRpcMethod#getId()}）判断该次下发是否走「自定义 JSON」：
     * {@code params} 即负载、不加 RPC 信封。
     * <p>
     * 方法不在目录（仪表板/规则链发起的 ad-hoc 调用）、档案尚未同步到本实例等情况下返回 {@code false}，
     * 调用方回退到默认下发形态。档案更新到会话拿到新 profile 之间有异步窗口，窗口内同样是这个安全方向。
     */
    public static boolean isCustomJsonDownlink(DeviceProfile profile, String methodName) {
        if (StringUtils.isBlank(methodName) || profile == null || profile.getProfileData() == null) {
            return false;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return false;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getId())) {
                return m.getBindingType() == DeviceProfileRpcBindingType.CUSTOM_JSON;
            }
        }
        return false;
    }
}
