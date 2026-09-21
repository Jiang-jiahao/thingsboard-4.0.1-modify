-- 品牌改造：把系统种子数据里的旧包名与旧类名前缀改成新的。
--
-- 01-jnks-iot-4.0.1.sql.gz 是从**上游**官方镜像装出来的库导出的，里面的
-- component_descriptor 仍带 org.thingsboard.* 包名和 Tb* 类名前缀；
-- 而本仓库的规则节点类已迁到 com.jnks.iot.*.JnksIot*。
--
-- 运行时两头都按 FQCN 对齐：AnnotationComponentDiscoveryService 把扫描到的
-- 类名写进 clazz，规则节点执行时走 Class.forName(rule_node.type)。
-- 两边对不上，新装出来的库在规则链编辑器里会一个节点都列不出来。
--
-- 两个列都要改：
--   clazz                   — 运行时按它匹配描述符
--   configuration_descriptor— 节点自带的 JSON，里面也存了一份完整类名
-- （应用启动后会按 Java 注解重新同步这两列，这里是让首次启动前也正确。）
--
-- 执行顺序不能动：必须排在 04-fork-data-fix.sql **之前**。那个脚本按
-- clazz LIKE 'com.jnks.iot.rule.engine.edge.%' 删已移除的 Edge 节点，依赖此处
-- 已经改过名；顺序反了 Edge 节点会漏删，编辑里继续列出不能用的节点。
--
-- rule_node 表不用动：上游导出时它是空的，规则链在租户创建时才按新类名生成。
--
-- 幂等：replace 对已改过的行不再匹配 WHERE，重复执行安全。

UPDATE component_descriptor
SET clazz = replace(replace(clazz, 'org.thingsboard', 'com.jnks.iot'), '.Tb', '.JnksIot'),
    configuration_descriptor = replace(replace(configuration_descriptor, 'org.thingsboard', 'com.jnks.iot'), '.Tb', '.JnksIot')
WHERE clazz LIKE 'org.thingsboard.%'
   OR clazz LIKE '%.Tb%'
   OR configuration_descriptor LIKE '%org.thingsboard.%'
   OR configuration_descriptor LIKE '%.Tb%';
