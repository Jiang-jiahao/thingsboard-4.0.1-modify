package com.jnks.iot.server.dao.sql.query;

public interface QueryLogComponent {

    void logQuery(SqlQueryContext ctx, String query, long duration);
}
