package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.TbActorMsg;
import com.jnks.iot.server.common.msg.TbMsg;

public interface RuleChainAwareMsg extends TbActorMsg {

	RuleChainId getRuleChainId();

	TbMsg getMsg();
	
}
