package com.jnks.iot.server.dao.nosql;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Data;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.cassandra.guava.GuavaSession;
import com.jnks.iot.server.dao.util.AsyncTask;

import java.util.function.Function;

/**
 * Created by ashvayka on 24.10.18.
 */
@Data
public class CassandraStatementTask implements AsyncTask {

    private final TenantId tenantId;
    private final GuavaSession session;
    private final Statement statement;

    public ListenableFuture<JnksIotResultSet> executeAsync(Function<Statement, JnksIotResultSetFuture> executeAsyncFunction) {
        return Futures.transform(session.executeAsync(statement),
                result -> new JnksIotResultSet(statement, result, executeAsyncFunction),
                MoreExecutors.directExecutor()
        );
    }

}
