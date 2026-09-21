package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;

public interface TenantAwareMsg extends JnksIotActorMsg {

	TenantId getTenantId();
	
}
