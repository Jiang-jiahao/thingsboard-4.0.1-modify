package org.thingsboard.server.service.mail;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.AbstractListeningExecutor;

/**
 * 邮件服务执行线程池
 * 通用的邮件发送任务（如告警通知、API用量警告等）。
 */
@Component
public class MailExecutorService extends AbstractListeningExecutor {

    @Value("${actors.rule.mail_thread_pool_size}")
    private int mailExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return mailExecutorThreadPoolSize;
    }

}
