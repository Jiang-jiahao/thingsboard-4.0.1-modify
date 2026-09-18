package org.thingsboard.server.service.mail;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.AbstractListeningExecutor;

/**
 * Executor have the sole purpose to send mails. It should be used only by Mail Service.
 * For other purposes please use the MailExecutorService component
 * 邮件服务内部专用，用于执行实际的邮件发送调用。
 * */
@Component
public class MailSenderInternalExecutorService extends AbstractListeningExecutor {

    @Value("${actors.rule.mail_thread_pool_size}")
    private int mailExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return mailExecutorThreadPoolSize;
    }

}
