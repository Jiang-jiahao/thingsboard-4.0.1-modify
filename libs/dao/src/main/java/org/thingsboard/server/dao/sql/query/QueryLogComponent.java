package org.thingsboard.server.dao.sql.query;

public interface QueryLogComponent {

    void logQuery(SqlQueryContext ctx, String query, long duration);
}
