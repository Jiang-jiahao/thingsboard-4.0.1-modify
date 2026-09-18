package org.thingsboard.rule.engine.api.notification;

import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.notification.targets.slack.SlackConversation;
import org.thingsboard.server.common.data.notification.targets.slack.SlackConversationType;

import java.util.List;

public interface SlackService {

    void sendMessage(TenantId tenantId, String token, String conversationId, String message);

    List<SlackConversation> listConversations(TenantId tenantId, String token, SlackConversationType conversationType);

    String getToken(TenantId tenantId);

}
