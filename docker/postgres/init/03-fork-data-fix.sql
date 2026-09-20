-- fork 相对上游 4.0.1 的**数据**增量修正。
--
-- 01-thingsboard-4.0.1.sql.gz 是从上游官方镜像装出来的库导出的，里面的系统种子数据
-- 仍按「有 Edge」的上游假设生成；而本仓库移除了 Edge（实体/API/UI/规则节点），
-- 于是这些种子数据会让相关接口直接报错。已发现的两种：
--
--   1) notification_rule：三条 RATE_LIMITS 系统规则里带着 EDGE_* 限流项，
--      而 LimitedApi 枚举已没有这些值 → GET /api/notification/rules 报
--      "The given object value cannot be converted to ... NotificationRuleTriggerConfig"
--   2) component_descriptor：push to edge / push to cloud 两个规则节点的描述符，
--      对应的 Java 类已不存在 → 规则链编辑器里会列出不能用的节点
--
-- 以后再从上游种子数据里发现类似引用（引用了本仓库删掉的能力），也补到这里。

-- 1) 通知规则：去掉 apis 里的 EDGE_* 项
UPDATE notification_rule
SET trigger_config = (
    jsonb_set(
        trigger_config::jsonb,
        '{apis}',
        coalesce(
            (SELECT jsonb_agg(v) FROM jsonb_array_elements(trigger_config::jsonb -> 'apis') v
             WHERE v::text NOT LIKE '%EDGE%'),
            '[]'::jsonb
        )
    )
)::text
WHERE trigger_config::jsonb ->> 'triggerType' = 'RATE_LIMITS';

-- 2) 已移除的 Edge 规则节点描述符
DELETE FROM component_descriptor WHERE clazz LIKE 'org.thingsboard.rule.engine.edge.%';
