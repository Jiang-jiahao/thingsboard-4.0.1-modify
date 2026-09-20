package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.TbActorMsg;

public interface TenantAwareMsg extends TbActorMsg {

	TenantId getTenantId();
	
}
