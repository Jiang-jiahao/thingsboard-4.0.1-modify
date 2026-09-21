-- 品牌改造：系统种子数据里**对外可见的文案**。
--
-- 03-rebrand-package.sql 改的是 component_descriptor.clazz（类名，运行时按 FQCN 匹配）；
-- 这里处理的是纯文本：部件名称与说明、OAuth 登录模板说明、默认管理员账号、JWT 签发者。
-- 同样是上游导出的种子数据，Docker 装库走的是这份二进制转储，sed 碰不到。
--
-- 顺序：本文件排在 05-rename-tb-tables.sql **之后**，所以这里用的是改名后的表名。
-- 幂等：WHERE 限定在仍含旧字样的行，重复执行是空操作。

-- ── 1) 部件元数据 ────────────────────────────────────────────────
-- 显示在规则链编辑器、部件库和仪表盘上的名称与说明
UPDATE widget_type
SET name        = replace(name,        'ThingsBoard', 'JnksIOT'),
    description = replace(description, 'ThingsBoard', 'JnksIOT'),
    descriptor  = replace(descriptor,  'ThingsBoard', 'JnksIOT')
WHERE name        LIKE '%ThingsBoard%'
   OR description LIKE '%ThingsBoard%'
   OR descriptor  LIKE '%ThingsBoard%';

UPDATE widgets_bundle
SET title       = replace(title,       'ThingsBoard', 'JnksIOT'),
    description = replace(description, 'ThingsBoard', 'JnksIOT')
WHERE title LIKE '%ThingsBoard%' OR description LIKE '%ThingsBoard%';

-- ── 2) OAuth 登录模板说明 ────────────────────────────────────────
UPDATE oauth2_client_registration_template
SET comment = replace(comment, 'ThingsBoard', 'JnksIOT')
WHERE comment LIKE '%ThingsBoard%';

-- ── 3) 默认系统管理员账号 ────────────────────────────────────────
-- 改的是邮箱（登录名），**密码不变，仍是 sysadmin**。
-- 若你们已对外公布过 sysadmin@thingsboard.org 这个账号，删掉这段。
UPDATE jnks_iot_user
SET email = replace(email, '@thingsboard.org', '@jnks-iot.org')
WHERE email LIKE '%@thingsboard.org';

-- ── 4) JWT 签发者 ────────────────────────────────────────────────
-- 这个值会写进每个 token 的 iss 字段，解码 token 就能看到。
-- 改它会让**已签发的 token 全部失效**（存量数据可重建，无影响）。
-- 应用侧的默认值在 apps/*/src/main/resources/jnks-iot*.yml 的 security.jwt.tokenIssuer。
UPDATE admin_settings
SET json_value = replace(json_value,
                         '"tokenIssuer":"thingsboard.io"',
                         '"tokenIssuer":"jnks-iot.org"')
WHERE key = 'jwt'
  AND json_value LIKE '%"tokenIssuer":"thingsboard.io"%';

-- ── 5) 邮件发件人显示名 ──────────────────────────────────────────
-- 系统发出的每封通知邮件都会显示这个名称。
-- 邮件地址（sysadmin@localhost.localdomain）是上游默认值，由你们在界面里配，这里不动。
UPDATE admin_settings
SET json_value = replace(json_value, '"mailFrom":"ThingsBoard ', '"mailFrom":"JnksIOT ')
WHERE key = 'mail'
  AND json_value LIKE '%"mailFrom":"ThingsBoard %';

-- ── 6) 规则节点帮助文本 ──────────────────────────────────────────
-- 03 只改了 clazz（运行时按 FQCN 匹配）；节点自带的帮助文本里还提到旧配置文件名。
-- 应用启动后会按 Java 注解里的描述重新同步这一列，这里是让首次启动前也正确。
UPDATE component_descriptor
SET configuration_descriptor = replace(configuration_descriptor, 'thingsboard.conf', 'jnks-iot.conf')
WHERE configuration_descriptor LIKE '%thingsboard.conf%';
