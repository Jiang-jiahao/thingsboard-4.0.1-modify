package com.jnks.iot.rule.engine.api.notification;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.targets.slack.SlackConversation;
import com.jnks.iot.server.common.data.notification.targets.slack.SlackConversationType;

import java.util.List;

public interface SlackService {

    void sendMessage(TenantId tenantId, String token, String conversationId, String message);

    List<SlackConversation> listConversations(TenantId tenantId, String token, SlackConversationType conversationType);

    String getToken(TenantId tenantId);

}
