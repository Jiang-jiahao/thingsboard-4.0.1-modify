-- fork 相对上游 4.0.1 多出来的 schema 增量。
--
-- 01-jnks-iot-4.0.1.sql.gz 是从**上游**官方镜像装出来的库导出的，不含本仓库自己加的表/列；
-- 应用侧 ddl-auto=none，不会自己建表，所以缺了这些对象时相关页面会直接 500
-- （例如协议模板页面：relation "protocol_template_bundle" does not exist）。
--
-- 内容摘自：
--   libs/dao/src/main/resources/sql/schema-entities.sql      （protocol_template_bundle）
--   apps/tb-core/src/main/data/upgrade/basic/schema_update.sql（api_usage_state.version）
-- 两边都写成 IF NOT EXISTS，重复执行安全。以后再往 schema 里加东西，记得同步补到这里。

CREATE TABLE IF NOT EXISTS protocol_template_bundle (
    id uuid NOT NULL CONSTRAINT protocol_template_bundle_pkey PRIMARY KEY,
    created_time bigint NOT NULL,
    tenant_id uuid NOT NULL,
    name varchar(255),
    description varchar(512),
    bundle_data jsonb NOT NULL,
    version BIGINT DEFAULT 1,
    CONSTRAINT protocol_template_bundle_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

ALTER TABLE api_usage_state ADD COLUMN IF NOT EXISTS version BIGINT DEFAULT 1;
