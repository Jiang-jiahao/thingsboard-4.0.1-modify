package com.jnks.iot.rule.engine.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.dao.exception.DataValidationException;

import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
public abstract class JnksIotAbstractCustomerActionNode<C extends JnksIotAbstractCustomerActionNodeConfiguration> implements JnksIotNode {

    private static final Set<EntityType> supportedEntityTypes = EnumSet.of(EntityType.ASSET, EntityType.DEVICE,
            EntityType.ENTITY_VIEW, EntityType.DASHBOARD);

    private static final String supportedEntityTypesStr = supportedEntityTypes.stream().map(Enum::name).collect(Collectors.joining(", "));

    protected C config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = loadCustomerNodeActionConfig(configuration);
    }

    protected abstract boolean createCustomerIfNotExists();

    protected abstract C loadCustomerNodeActionConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException;

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var entityType = msg.getOriginator().getEntityType();
        if (!supportedEntityTypes.contains(entityType)) {
            throw new RuntimeException(unsupportedOriginatorTypeErrorMessage(entityType));
        }
        withCallback(processCustomerAction(ctx, msg),
                m -> ctx.tellSuccess(msg),
                t -> ctx.tellFailure(msg, t), MoreExecutors.directExecutor());
    }

    protected abstract ListenableFuture<Void> processCustomerAction(JnksIotContext ctx, JnksIotMsg msg);

    protected ListenableFuture<CustomerId> getCustomerIdFuture(JnksIotContext ctx, JnksIotMsg msg) {
        var tenantId = ctx.getTenantId();
        var customerTitle = JnksIotNodeUtils.processPattern(this.config.getCustomerNamePattern(), msg);
        var customerService = ctx.getCustomerService();
        var customerByTitleFuture = customerService.findCustomerByTenantIdAndTitleAsync(tenantId, customerTitle);
        if (createCustomerIfNotExists()) {
            return Futures.transform(customerByTitleFuture, customerOpt -> {
                if (customerOpt.isPresent()) {
                    return customerOpt.get().getId();
                }
                try {
                    var newCustomer = new Customer();
                    newCustomer.setTitle(customerTitle);
                    newCustomer.setTenantId(tenantId);
                    var savedCustomer = customerService.saveCustomer(newCustomer);
                    ctx.enqueue(ctx.customerCreatedMsg(savedCustomer, ctx.getSelfId()),
                            () -> log.trace("Pushed Customer Created message: {}", savedCustomer),
                            throwable -> log.warn("Failed to push Customer Created message: {}", savedCustomer, throwable));
                    return savedCustomer.getId();
                } catch (DataValidationException e) {
                    customerOpt = customerService.findCustomerByTenantIdAndTitle(tenantId, customerTitle);
                    if (customerOpt.isPresent()) {
                        return customerOpt.get().getId();
                    }
                    throw new RuntimeException("Failed to create customer with title '" + customerTitle + "' due to: ", e);
                }
            }, MoreExecutors.directExecutor());
        }
        return Futures.transform(customerByTitleFuture, customerOpt -> {
            if (customerOpt.isEmpty()) {
                throw new NoSuchElementException("Customer with title '" + customerTitle + "' doesn't exist!");
            }
            return customerOpt.get().getId();
        }, MoreExecutors.directExecutor());
    }

    private static String unsupportedOriginatorTypeErrorMessage(EntityType originatorType) {
        return "Unsupported originator type '" + originatorType +
                "'! Only " + supportedEntityTypesStr + " types are allowed.";
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0 -> {
                if (oldConfiguration.has("customerCacheExpiration")) {
                    ((ObjectNode) oldConfiguration).remove("customerCacheExpiration");
                    hasChanges = true;
                }
            }
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
