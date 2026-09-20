-- 品牌改造：把系统种子数据里的旧包名改成新包名。
--
-- 01-jnks-iot-4.0.1.sql.gz 是从**上游**官方镜像装出来的库导出的，里面的
-- component_descriptor.clazz 仍是 org.thingsboard.rule.engine.*；而本仓库的
-- 规则节点类已迁到 com.jnks.iot.rule.engine.*。
--
-- 运行时两头都按 FQCN 对齐：AnnotationComponentDiscoveryService 把扫描到的
-- 类名写进 component_descriptor.clazz，规则节点执行时走 Class.forName(
-- rule_node.type)。两边对不上，新装出来的库在规则链编辑器里会一个节点都列不出来。
--
-- 执行顺序不能动：必须排在 04-fork-data-fix.sql **之前**。那个脚本按
-- clazz LIKE 'com.jnks.iot.rule.engine.edge.%' 删已移除的 Edge 节点，依赖此处
-- 已经改过名；顺序反了 Edge 节点会漏删，编辑里继续列出不能用的节点。
--
-- rule_node 表不用动：上游导出时它是空的，规则链在租户创建时才按新类名生成。
--
-- 幂等：replace 对已改过的行不再匹配 WHERE，重复执行安全。

UPDATE component_descriptor
SET clazz = replace(clazz, 'org.thingsboard', 'com.jnks.iot')
WHERE clazz LIKE 'org.thingsboard.%';
