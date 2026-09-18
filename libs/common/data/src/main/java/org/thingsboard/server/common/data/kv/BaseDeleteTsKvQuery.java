package org.thingsboard.server.common.data.kv;

import lombok.Data;

@Data
public class BaseDeleteTsKvQuery extends BaseTsKvQuery implements DeleteTsKvQuery {

    private final Boolean rewriteLatestIfDeleted;
    private final Boolean deleteLatest;

    public BaseDeleteTsKvQuery(String key, long startTs, long endTs, boolean rewriteLatestIfDeleted, boolean deleteLatest) {
        super(key, startTs, endTs);
        this.rewriteLatestIfDeleted = rewriteLatestIfDeleted;
        this.deleteLatest = deleteLatest;
    }

    public BaseDeleteTsKvQuery(String key, long startTs, long endTs, boolean rewriteLatestIfDeleted) {
        this(key, startTs, endTs, rewriteLatestIfDeleted, true);
    }

    public BaseDeleteTsKvQuery(String key, long startTs, long endTs) {
        this(key, startTs, endTs, false, true);
    }

}
