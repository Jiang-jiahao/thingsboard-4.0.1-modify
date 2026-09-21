package com.jnks.iot.server.common.msg.aware;

import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotMsg;

public interface RuleChainAwareMsg extends JnksIotActorMsg {

	RuleChainId getRuleChainId();

	JnksIotMsg getMsg();
	
}
