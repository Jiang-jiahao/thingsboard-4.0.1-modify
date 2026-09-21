package com.jnks.iot.rule.engine.action;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.rule.RuleChainType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.dao.cassandra.CassandraCluster;
import com.jnks.iot.server.dao.cassandra.guava.GuavaSession;
import com.jnks.iot.server.dao.nosql.CassandraStatementTask;
import com.jnks.iot.server.dao.nosql.JnksIotResultSetFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(type = ComponentType.ACTION,
        name = "保存到自定义表",
        configClazz = JnksIotSaveToCustomCassandraTableNodeConfiguration.class,
        version = 1,
        nodeDescription = "节点将传入消息负载中的数据存储到 Cassandra 数据库的预定义自定义表中" +
                " 该表应带有 <b>cs_tb_</b> 前缀，以避免将数据插入到公共的 TB 表中。<br>" +
                "<b>注意：</b>该规则节点只能用于 Cassandra 数据库。",
        nodeDetails = "管理员应设置不带前缀的自定义表名：<b>cs_tb_</b>。 <br>" +
                "管理员可以配置消息字段名与表列名之间的映射。<br>" +
                "<b>注意：</b>如果映射键为 <b>$entity_id</b>（由消息来源方标识），则会向对应的列名（映射值）写入消息来源方 id。<br><br>" +
                "如果指定的消息字段不存在或不是 JSON Primitive，则出站消息将经由 <b>failure</b> 链路由，" +
                " 否则，消息将经由 <b>success</b> 链路由。",
        configDirective = "jnksIotActionNodeCustomTableConfig",
        icon = "file_upload",
        ruleChainTypes = RuleChainType.CORE)
public class JnksIotSaveToCustomCassandraTableNode implements JnksIotNode {

    private static final String TABLE_PREFIX = "cs_tb_";
    private static final String ENTITY_ID = "$entityId";

    private JnksIotSaveToCustomCassandraTableNodeConfiguration config;
    private GuavaSession session;
    private CassandraCluster cassandraCluster;
    private ConsistencyLevel defaultWriteLevel;
    private PreparedStatement saveStmt;
    private ExecutorService readResultsProcessingExecutor;
    private Map<String, String> fieldsMap;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        config = JnksIotNodeUtils.convert(configuration, JnksIotSaveToCustomCassandraTableNodeConfiguration.class);
        cassandraCluster = ctx.getCassandraCluster();
        if (cassandraCluster == null) {
            throw new JnksIotNodeException("Unable to connect to Cassandra database", true);
        }
        if (!isTableExists()) {
            throw new JnksIotNodeException("Table '" + TABLE_PREFIX + config.getTableName() + "' does not exist in Cassandra cluster.");
        }
        startExecutor();
        saveStmt = getSaveStmt();
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        withCallback(save(msg, ctx), aVoid -> ctx.tellSuccess(msg), e -> ctx.tellFailure(msg, e), ctx.getDbCallbackExecutor());
    }

    @Override
    public void destroy() {
        stopExecutor();
        saveStmt = null;
    }

    private void startExecutor() {
        readResultsProcessingExecutor = Executors.newCachedThreadPool();
    }

    private void stopExecutor() {
        if (readResultsProcessingExecutor != null) {
            readResultsProcessingExecutor.shutdownNow();
        }
    }

    private boolean isTableExists() {
        var keyspaceMdOpt = getSession().getMetadata().getKeyspace(cassandraCluster.getKeyspaceName());
        return keyspaceMdOpt.map(keyspaceMetadata ->
                keyspaceMetadata.getTable(TABLE_PREFIX + config.getTableName()).isPresent()).orElse(false);
    }

    private PreparedStatement prepare(String query) {
        return getSession().prepare(query);
    }

    private GuavaSession getSession() {
        if (session == null) {
            session = cassandraCluster.getSession();
            defaultWriteLevel = cassandraCluster.getDefaultWriteConsistencyLevel();
        }
        return session;
    }

    private PreparedStatement getSaveStmt() throws JnksIotNodeException {
        fieldsMap = config.getFieldsMapping();
        if (fieldsMap.isEmpty()) {
            throw new JnksIotNodeException("Fields(key,value) map is empty!", true);
        } else {
            return prepareStatement(new ArrayList<>(fieldsMap.values()));
        }
    }

    private PreparedStatement prepareStatement(List<String> fieldsList) {
        return prepare(createQuery(fieldsList));
    }

    private String createQuery(List<String> fieldsList) {
        int size = fieldsList.size();
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ")
                .append(TABLE_PREFIX)
                .append(config.getTableName())
                .append("(");
        for (String field : fieldsList) {
            query.append(field);
            if (fieldsList.get(size - 1).equals(field)) {
                query.append(")");
            } else {
                query.append(",");
            }
        }
        query.append(" VALUES(");
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                query.append("?)");
            } else {
                query.append("?, ");
            }
        }
        if (config.getDefaultTtl() > 0) {
            query.append(" USING TTL ?");
        }
        return query.toString();
    }

    private ListenableFuture<Void> save(JnksIotMsg msg, JnksIotContext ctx) {
        JsonElement data = JsonParser.parseString(msg.getData());
        if (!data.isJsonObject()) {
            throw new IllegalStateException("Invalid message structure, it is not a JSON Object: " + data);
        } else {
            JsonObject dataAsObject = data.getAsJsonObject();
            BoundStatementBuilder stmtBuilder = getStmtBuilder();
            AtomicInteger i = new AtomicInteger(0);
            fieldsMap.forEach((key, value) -> {
                if (key.equals(ENTITY_ID)) {
                    stmtBuilder.setUuid(i.get(), msg.getOriginator().getId());
                } else if (dataAsObject.has(key)) {
                    JsonElement dataKeyElement = dataAsObject.get(key);
                    if (dataKeyElement.isJsonPrimitive()) {
                        JsonPrimitive primitive = dataKeyElement.getAsJsonPrimitive();
                        if (primitive.isNumber()) {
                            if (primitive.getAsString().contains(".")) {
                                stmtBuilder.setDouble(i.get(), primitive.getAsDouble());
                            } else {
                                stmtBuilder.setLong(i.get(), primitive.getAsLong());
                            }
                        } else if (primitive.isBoolean()) {
                            stmtBuilder.setBoolean(i.get(), primitive.getAsBoolean());
                        } else if (primitive.isString()) {
                            stmtBuilder.setString(i.get(), primitive.getAsString());
                        } else {
                            stmtBuilder.setToNull(i.get());
                        }
                    } else if (dataKeyElement.isJsonObject()) {
                        stmtBuilder.setString(i.get(), dataKeyElement.getAsJsonObject().toString());
                    } else {
                        throw new IllegalStateException("Message data key: '" + key + "' with value: '" + dataKeyElement + "' is not a JSON Object or JSON Primitive!");
                    }
                } else {
                    throw new RuntimeException("Message data doesn't contain key: " + "'" + key + "'!");
                }
                i.getAndIncrement();
            });
            if (config.getDefaultTtl() > 0) {
                stmtBuilder.setInt(i.get(), config.getDefaultTtl());
            }
            return getFuture(executeAsyncWrite(ctx, stmtBuilder.build()), rs -> null);
        }
    }

    BoundStatementBuilder getStmtBuilder() {
        return new BoundStatementBuilder(saveStmt.bind());
    }

    private JnksIotResultSetFuture executeAsyncWrite(JnksIotContext ctx, Statement statement) {
        return executeAsync(ctx, statement, defaultWriteLevel);
    }

    private JnksIotResultSetFuture executeAsync(JnksIotContext ctx, Statement statement, ConsistencyLevel level) {
        if (log.isDebugEnabled()) {
            log.debug("Execute cassandra async statement {}", statementToString(statement));
        }
        if (statement.getConsistencyLevel() == null) {
            statement.setConsistencyLevel(level);
        }
        return ctx.submitCassandraWriteTask(new CassandraStatementTask(ctx.getTenantId(), getSession(), statement));
    }

    private static String statementToString(Statement statement) {
        if (statement instanceof BoundStatement) {
            return ((BoundStatement) statement).getPreparedStatement().getQuery();
        } else {
            return statement.toString();
        }
    }

    private <T> ListenableFuture<T> getFuture(JnksIotResultSetFuture future, java.util.function.Function<AsyncResultSet, T> transformer) {
        return Futures.transform(future, new Function<AsyncResultSet, T>() {
            @Nullable
            @Override
            public T apply(@Nullable AsyncResultSet input) {
                return transformer.apply(input);
            }
        }, readResultsProcessingExecutor);
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                if (!oldConfiguration.has("defaultTtl")) {
                    hasChanges = true;
                    ((ObjectNode) oldConfiguration).put("defaultTtl", 0);
                }
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
