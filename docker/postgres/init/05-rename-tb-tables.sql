-- 品牌改造：把上游遗留的 tb_ 前缀表名、约束名、列名改成 jnks_iot_。
--
-- 为什么单独一步：01 的转储是**二进制**，sed 碰不到里面的表名，只能靠 ALTER 改。
--
-- 改完不会破坏什么：
--   - 视图与外键在 PostgreSQL 里按 OID 引用对象，RENAME TABLE 后自动跟随
--   - 实测转储里 11 个函数体都没有引用这些名字（函数是运行时解析文本，才需要重建）
--
-- 执行顺序：必须排在 06-rebrand-seed-text.sql **之前**——那个脚本要按新表名
-- 更新默认管理员邮箱。所以本文件编号 05，原 05 顺延为 06。
--
-- 幂等：RENAME 本身不幂等，全部用存在性判断包起来，重复执行安全。

-- ── 表名 ────────────────────────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'tb_user') THEN
        ALTER TABLE public.tb_user RENAME TO jnks_iot_user;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'tb_schema_settings') THEN
        ALTER TABLE public.tb_schema_settings RENAME TO jnks_iot_schema_settings;
    END IF;
END $$;

-- ── 约束名 ──────────────────────────────────────────────────────
-- 约束名不跟随表改名，得单独改。它会出现在 PostgreSQL 的报错文本里
-- （例如唯一键冲突：「重复键违反唯一约束 "tb_user_email_key"」），
-- 所以 Java 侧 AbstractEntityService 的注释也一并改了。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tb_user_pkey') THEN
        ALTER TABLE public.jnks_iot_user RENAME CONSTRAINT tb_user_pkey TO jnks_iot_user_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tb_user_email_key') THEN
        ALTER TABLE public.jnks_iot_user RENAME CONSTRAINT tb_user_email_key TO jnks_iot_user_email_key;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tb_schema_settings_pkey') THEN
        ALTER TABLE public.jnks_iot_schema_settings RENAME CONSTRAINT tb_schema_settings_pkey TO jnks_iot_schema_settings_pkey;
    END IF;
END $$;

-- ── 列名 ────────────────────────────────────────────────────────
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'tenant_profile'
                 AND column_name = 'isolated_tb_core') THEN
        ALTER TABLE public.tenant_profile RENAME COLUMN isolated_tb_core TO isolated_jnks_iot_core;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'tenant_profile'
                 AND column_name = 'isolated_tb_rule_engine') THEN
        ALTER TABLE public.tenant_profile RENAME COLUMN isolated_tb_rule_engine TO isolated_jnks_iot_rule_engine;
    END IF;
END $$;
