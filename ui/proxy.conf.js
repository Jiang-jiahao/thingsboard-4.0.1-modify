/**
 * 后端未启动或端口不对时，开发服务器会反复报 [vite] ws proxy error / AggregateError
 *（Node 对 localhost 可能先试 IPv6 再试 IPv4，连接被拒会打多条）。
 * 解决：先启动 ThingsBoard（默认 8080），或设置环境变量覆盖，例如：
 *   set TB_PROXY_TARGET=http://127.0.0.1:8080 && yarn start
 *   TB_PROXY_TARGET=http://192.168.1.10:8080 yarn start
 */
const forwardUrl = process.env.TB_PROXY_TARGET || "http://127.0.0.1:8080";
const wsForwardUrl = forwardUrl.replace(/^https:\/\//i, "wss://").replace(/^http:\/\//i, "ws://");
const ruleNodeUiforwardUrl = forwardUrl;

const PROXY_CONFIG = {
  "/api": {
    "target": forwardUrl,
    "secure": false,
  },
  "/static/rulenode": {
    "target": ruleNodeUiforwardUrl,
    "secure": false,
  },
  "/static/widgets": {
    "target": forwardUrl,
    "secure": false,
  },
  "/oauth2": {
    "target": forwardUrl,
    "secure": false,
  },
  "/login/oauth2": {
    "target": forwardUrl,
    "secure": false,
  },
  "/api/ws": {
    "target": wsForwardUrl,
    "ws": true,
    "secure": false
  },
};

module.exports = PROXY_CONFIG;
