package org.thingsboard.server.service.mail;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.AbstractListeningExecutor;

/**
 * 密码重置流程专用，确保密码重置邮件的高优先级与可靠性。
 */
@Component
public class PasswordResetExecutorService extends AbstractListeningExecutor {

    @Value("${actors.rule.mail_password_reset_thread_pool_size:10}")
    private int mailExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return mailExecutorThreadPoolSize;
    }

}
