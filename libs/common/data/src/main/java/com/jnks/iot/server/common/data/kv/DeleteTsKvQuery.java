package com.jnks.iot.server.common.data.kv;

public interface DeleteTsKvQuery extends TsKvQuery {

    Boolean getRewriteLatestIfDeleted();

    Boolean getDeleteLatest();

}
