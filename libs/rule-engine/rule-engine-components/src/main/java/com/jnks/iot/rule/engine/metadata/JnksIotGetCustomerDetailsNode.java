package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.HasCustomerId;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.NoSuchElementException;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "客户详情",
        configClazz = JnksIotGetCustomerDetailsNodeConfiguration.class,
        version = 1,
        nodeDescription = "将消息来源方的客户详情添加到消息或消息元数据中",
        nodeDetails = "适用于需要动态使用客户联系信息的多客户解决方案， " +
                "例如电子邮件、电话、地址等，用于通过电子邮件、短信和其他通知渠道发送通知。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeEntityDetailsConfig")
public class JnksIotGetCustomerDetailsNode extends JnksIotAbstractGetEntityDetailsNode<JnksIotGetCustomerDetailsNodeConfiguration, CustomerId> {

    private static final String CUSTOMER_PREFIX = "customer_";

    @Override
    protected JnksIotGetCustomerDetailsNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        var config = JnksIotNodeUtils.convert(configuration, JnksIotGetCustomerDetailsNodeConfiguration.class);
        checkIfDetailsListIsNotEmptyOrElseThrow(config.getDetailsList());
        return config;
    }

    @Override
    protected String getPrefix() {
        return CUSTOMER_PREFIX;
    }

    @Override
    protected ListenableFuture<Customer> getContactBasedFuture(JnksIotContext ctx, JnksIotMsg msg) {
        switch (msg.getOriginator().getEntityType()) {
            case DEVICE:
                return Futures.transformAsync(ctx.getDeviceService().findDeviceByIdAsync(ctx.getTenantId(), new DeviceId(msg.getOriginator().getId())),
                        device -> getCustomerFuture(ctx, device, msg.getOriginator()), ctx.getDbCallbackExecutor());
            case ASSET:
                return Futures.transformAsync(ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), new AssetId(msg.getOriginator().getId())),
                        asset -> getCustomerFuture(ctx, asset, msg.getOriginator()), ctx.getDbCallbackExecutor());
            case ENTITY_VIEW:
                return Futures.transformAsync(ctx.getEntityViewService().findEntityViewByIdAsync(ctx.getTenantId(), new EntityViewId(msg.getOriginator().getId())),
                        entityView -> getCustomerFuture(ctx, entityView, msg.getOriginator()), ctx.getDbCallbackExecutor());
            case USER:
                return Futures.transformAsync(ctx.getUserService().findUserByIdAsync(ctx.getTenantId(), new UserId(msg.getOriginator().getId())),
                        user -> getCustomerFuture(ctx, user, msg.getOriginator()), ctx.getDbCallbackExecutor());
            default:
                return Futures.immediateFailedFuture(new NoSuchElementException("Entity with entityType '" + msg.getOriginator().getEntityType() + "' is not supported."));
        }
    }

    private ListenableFuture<Customer> getCustomerFuture(JnksIotContext ctx, HasCustomerId hasCustomerId, EntityId originator) {
        if (hasCustomerId == null) {
            return Futures.immediateFuture(null);
        } else {
            if (hasCustomerId.getCustomerId() == null || hasCustomerId.getCustomerId().isNullUid()) {
                if (hasCustomerId instanceof HasName) {
                    var hasName = (HasName) hasCustomerId;
                    throw new RuntimeException(originator.getEntityType().getNormalName() + " with name '" + hasName.getName() + "' is not assigned to Customer!");
                }
                throw new RuntimeException(originator.getEntityType().getNormalName() + " with id '" + originator + "' is not assigned to Customer!");
            } else {
                return ctx.getCustomerService().findCustomerByIdAsync(ctx.getTenantId(), hasCustomerId.getCustomerId());
            }
        }
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "addToMetadata",
                        JnksIotMsgSource.METADATA.name(),
                        JnksIotMsgSource.DATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
