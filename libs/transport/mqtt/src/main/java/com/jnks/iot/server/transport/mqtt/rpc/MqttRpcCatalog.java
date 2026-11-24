package com.jnks.iot.server.transport.mqtt.rpc;

import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcBindingType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileRpcMethod;
import com.jnks.iot.server.common.transport.auth.TransportDeviceInfo;

import java.util.List;

public final class MqttRpcCatalog {

    private MqttRpcCatalog() {
    }

    public static DeviceProfileRpcMethod find(DeviceProfile profile, String methodName) {
        if (StringUtils.isBlank(methodName) || profile == null || profile.getProfileData() == null) {
            return null;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return null;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getId())) {
                return m;
            }
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getDeviceMethod())) {
                return m;
            }
        }
        return null;
    }

    public static boolean isMqttRpcBinding(DeviceProfileRpcMethod method) {
        if (method == null || method.getBindingType() == null) {
            return false;
        }
        return method.getBindingType() == DeviceProfileRpcBindingType.NATIVE
                || method.getBindingType() == DeviceProfileRpcBindingType.MQTT_CUSTOM;
    }

    public static boolean isCustomRequestSubscribeTopic(DeviceProfile profile, Device device,
                                                        TransportDeviceInfo deviceInfo, String topic) {
        if (StringUtils.isBlank(topic) || profile == null || profile.getProfileData() == null) {
            return false;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return false;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (!isMqttRpcBinding(m) || StringUtils.isBlank(m.getMqttRequestTopic())) {
                continue;
            }
            String filter = MqttRpcTemplateResolver.toRequestSubscribeFilter(m.getMqttRequestTopic(), device, deviceInfo);
            if (MqttRpcCommandFactory.topicMatches(filter, topic)
                    || topicEqualsOrCovers(topic, filter)
                    || topicFiltersOverlap(filter, topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 两个主题过滤器是否有交集。设备订阅可能带 {@code #}/{@code +}，而档案主题里的
     * {@code ${params.x}} 会被替换成 {@code +}；只做字符串前缀比较会漏判
     * （例如设备订阅 {@code .../{sn}/commands/#}，档案主题是 {@code .../+/commands/directed-strike}）。
     */
    static boolean topicFiltersOverlap(String filter, String subscription) {
        if (StringUtils.isBlank(filter) || StringUtils.isBlank(subscription)) {
            return false;
        }
        if (filter.equals(subscription)) {
            return true;
        }
        String[] fp = filter.split("/", -1);
        String[] sp = subscription.split("/", -1);
        int i = 0;
        while (i < fp.length && i < sp.length) {
            String fs = fp[i];
            String ss = sp[i];
            if ("#".equals(fs) || "#".equals(ss)) {
                return true;
            }
            if (!fs.equals(ss) && !"+".equals(fs) && !"+".equals(ss)) {
                return false;
            }
            i++;
        }
        if (fp.length == sp.length) {
            return true;
        }
        String[] longer = fp.length > sp.length ? fp : sp;
        return "#".equals(longer[i]);
    }

    private static boolean topicEqualsOrCovers(String subscription, String filter) {
        if (subscription == null || filter == null) {
            return false;
        }
        if (subscription.equals(filter)) {
            return true;
        }
        if (subscription.endsWith("/#")) {
            String prefix = subscription.substring(0, subscription.length() - 2);
            return filter.equals(prefix) || filter.startsWith(prefix + "/");
        }
        if (subscription.contains("+")) {
            return MqttRpcCommandFactory.topicMatches(subscription, filter);
        }
        return false;
    }
}
