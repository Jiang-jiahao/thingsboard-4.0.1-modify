package com.jnks.iot.rule.engine.action;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ConcurrentReferenceHashMap;
import com.jnks.iot.rule.engine.api.DeviceStateManager;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.tools.JnksIotRateLimits;

import java.util.EnumSet;
import java.util.Set;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "设备状态",
        nodeDescription = "触发设备连接事件",
        nodeDetails = "若传入消息来源方是设备，则在设备状态服务中为该设备注册配置的事件，设备状态服务会向规则引擎发送相应的消息。" +
                " 若存在元数据 <code>ts</code> 属性，则将其用作事件时间戳。否则，将使用消息时间戳。" +
                " 若来源方实体类型不是 <code>DEVICE</code>，或处理过程中发生意外错误，则传入消息将通过 <code>Failure</code> 链转发。" +
                " 若给定来源方的连接事件频率过高，则传入消息将通过 <code>Rate limited</code> 链转发。 " +
                "<br>" +
                "支持的设备连接事件包括：" +
                "<ul>" +
                "<li>连接事件</li>" +
                "<li>断开连接事件</li>" +
                "<li>活动事件</li>" +
                "<li>不活动事件</li>" +
                "</ul>" +
                "当设备未使用传输（transports）接收数据时，此节点特别有用，例如从外部 API 获取数据或在规则链内计算新数据时。",
        configClazz = JnksIotDeviceStateNodeConfiguration.class,
        relationTypes = {JnksIotNodeConnectionType.SUCCESS, JnksIotNodeConnectionType.FAILURE, "Rate limited"},
        configDirective = "jnksIotActionNodeDeviceStateConfig"
)
public class JnksIotDeviceStateNode implements JnksIotNode {

    private static final Set<JnksIotMsgType> SUPPORTED_EVENTS = EnumSet.of(
            JnksIotMsgType.CONNECT_EVENT, JnksIotMsgType.ACTIVITY_EVENT, JnksIotMsgType.DISCONNECT_EVENT, JnksIotMsgType.INACTIVITY_EVENT
    );
    private static final String DEFAULT_RATE_LIMIT_CONFIG = "1:1,30:60,60:3600";
    private ConcurrentReferenceHashMap<DeviceId, JnksIotRateLimits> rateLimits;
    private String rateLimitConfig;
    private JnksIotMsgType event;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        JnksIotMsgType event = JnksIotNodeUtils.convert(configuration, JnksIotDeviceStateNodeConfiguration.class).getEvent();
        if (event == null) {
            throw new JnksIotNodeException("Event cannot be null!", true);
        }
        if (!SUPPORTED_EVENTS.contains(event)) {
            throw new JnksIotNodeException("Unsupported event: " + event, true);
        }
        this.event = event;
        rateLimits = new ConcurrentReferenceHashMap<>();
        String deviceStateNodeRateLimitConfig = ctx.getDeviceStateNodeRateLimitConfig();
        try {
            rateLimitConfig = new JnksIotRateLimits(deviceStateNodeRateLimitConfig).getConfiguration();
        } catch (Exception e) {
            log.error("[{}][{}] Invalid rate limit configuration provided: [{}]. Will use default value [{}].",
                    ctx.getTenantId().getId(), ctx.getSelfId().getId(), deviceStateNodeRateLimitConfig, DEFAULT_RATE_LIMIT_CONFIG, e);
            rateLimitConfig = DEFAULT_RATE_LIMIT_CONFIG;
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        EntityType originatorEntityType = msg.getOriginator().getEntityType();
        if (!EntityType.DEVICE.equals(originatorEntityType)) {
            ctx.tellFailure(msg, new IllegalArgumentException(
                    "Unsupported originator entity type: [" + originatorEntityType + "]. Only DEVICE entity type is supported."
            ));
            return;
        }
        DeviceId originator = new DeviceId(msg.getOriginator().getId());
        rateLimits.compute(originator, (__, rateLimit) -> {
            if (rateLimit == null) {
                rateLimit = new JnksIotRateLimits(rateLimitConfig);
            }
            boolean isNotRateLimited = rateLimit.tryConsume();
            if (isNotRateLimited) {
                sendEventAndTell(ctx, originator, msg);
            } else {
                ctx.tellNext(msg, "Rate limited");
            }
            return rateLimit;
        });
    }

    private void sendEventAndTell(JnksIotContext ctx, DeviceId originator, JnksIotMsg msg) {
        TenantId tenantId = ctx.getTenantId();
        long eventTs = msg.getMetaDataTs();

        DeviceStateManager deviceStateManager = ctx.getDeviceStateManager();
        JnksIotCallback callback = getMsgEnqueuedCallback(ctx, msg);

        switch (event) {
            case CONNECT_EVENT:
                deviceStateManager.onDeviceConnect(tenantId, originator, eventTs, callback);
                break;
            case ACTIVITY_EVENT:
                deviceStateManager.onDeviceActivity(tenantId, originator, eventTs, callback);
                break;
            case DISCONNECT_EVENT:
                deviceStateManager.onDeviceDisconnect(tenantId, originator, eventTs, callback);
                break;
            case INACTIVITY_EVENT:
                deviceStateManager.onDeviceInactivity(tenantId, originator, eventTs, callback);
                break;
            default:
                ctx.tellFailure(msg, new IllegalStateException("Configured event [" + event + "] is not supported!"));
        }
    }

    private JnksIotCallback getMsgEnqueuedCallback(JnksIotContext ctx, JnksIotMsg msg) {
        return new JnksIotCallback() {
            @Override
            public void onSuccess() {
                ctx.tellSuccess(msg);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(msg, t);
            }
        };
    }

    @Override
    public void onPartitionChangeMsg(JnksIotContext ctx, PartitionChangeMsg msg) {
        rateLimits.entrySet().removeIf(entry -> !ctx.isLocalEntity(entry.getKey()));
    }

    @Override
    public void destroy() {
        if (rateLimits != null) {
            rateLimits.clear();
            rateLimits = null;
        }
        rateLimitConfig = null;
        event = null;
    }

}
