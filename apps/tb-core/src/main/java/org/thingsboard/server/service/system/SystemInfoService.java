package org.thingsboard.server.service.system;

import org.thingsboard.server.common.data.FeaturesInfo;
import org.thingsboard.server.common.data.SystemInfo;

/**
 * 系统信息查询接口：集群/单机运行指标与功能开关状态。
 */
public interface SystemInfoService {
    /** 获取当前系统/集群资源占用信息。 */
    SystemInfo getSystemInfo();

    /** 获取邮件、短信、OAuth2、2FA、Slack 等功能是否已配置。 */
    FeaturesInfo getFeaturesInfo();
}
