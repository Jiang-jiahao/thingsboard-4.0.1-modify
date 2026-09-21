-- 品牌改造：把库里的部件数据从 tb- / Tb 前缀改成 jnks-iot- / JnksIot。
--
-- 为什么必须单独一步：**部件的定义存在数据库里**，不在代码里。
-- widget_type.descriptor 是部件的完整 JSON（模板 HTML、CSS、controllerScript、
-- settingsDirective），里面有三种跟代码强耦合的标识：
--
--   1. 组件选择器     <tb-markdown-widget>      ← 代码里已改为 jnks-iot-markdown-widget
--   2. 设置指令名     tb-charts-widget-settings ← 同上
--   3. 全局 JS API    TbTimeSeriesChart / TbFlot / TbMapWidgetV2 …
--                     ← 代码里 widget 作者可用的全局对象已改名为 JnksIot*
--   4. 图片引用前缀   tb-image;/api/images/…    ← 代码里已改为 jnks-iot-image;
--
-- 01 的转储来自上游，这些值全是旧的；而 UI 代码经品牌改造后用的是新名。
-- 两边对不上时，Angular 动态组件建不出来 —— 表现为**所有部件一直转圈**，
-- 首页、仪表盘、部件预览全线失效，且控制台不一定有报错，很难查。
--
-- 幂等：replace 对已改过的行不再命中。

-- 1) 组件选择器 / 设置指令名 / CSS 类名
UPDATE widget_type
SET descriptor = replace(descriptor, 'tb-', 'jnks-iot-')
WHERE descriptor LIKE '%tb-%';

-- 2) 全局 JS API（Tb 后面跟大写字母才替换，避免误伤 base64 等）
UPDATE widget_type
SET descriptor = regexp_replace(descriptor, '\mTb(?=[A-Z])', 'JnksIot', 'g')
WHERE descriptor ~ '\mTb[A-Z]';

-- 3) 图片引用前缀
UPDATE widget_type
SET image = replace(image, 'tb-image;', 'jnks-iot-image;')
WHERE image LIKE 'tb-image;%';

UPDATE widgets_bundle
SET image = replace(image, 'tb-image;', 'jnks-iot-image;')
WHERE image LIKE 'tb-image;%';
