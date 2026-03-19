/**
 * Copyright (c) 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors.tenant;

import org.thingsboard.server.actors.TbActorCtx;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.msg.TbActorMsg;

/**
 * Encapsulates rule-engine specific actor logic for tenant actor.
 */
public interface TenantRuleEngineActorSupport {

    TbActorRef getOrCreateCalculatedFieldManagerActor(TbActorCtx ctx);

    void initRuleChains(TbActorCtx ctx);

    void destroyRuleChains(TbActorCtx ctx);

    boolean isRuleChainsInitialized();

    TbActorRef getRootChainActor();

    TbActorRef getOrCreateRuleChainActor(TbActorCtx ctx, RuleChainId ruleChainId);

    TbActorRef getEntityActorRef(TbActorCtx ctx, EntityId entityId);

    void visit(RuleChain ruleChain, TbActorRef actorRef);

    void broadcastToRuleChains(TbActorCtx ctx, TbActorMsg msg);

}
