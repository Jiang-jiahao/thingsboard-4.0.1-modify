package com.jnks.iot.server.common.msg;

import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.msg.gen.MsgProtos;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ashvayka on 13.01.18.
 */
public final class JnksIotMsgProcessingCtx implements Serializable {

    private final AtomicInteger ruleNodeExecCounter;
    private volatile LinkedList<JnksIotMsgProcessingStackItem> stack;

    public JnksIotMsgProcessingCtx() {
        this(0);
    }

    public JnksIotMsgProcessingCtx(int ruleNodeExecCounter) {
        this(ruleNodeExecCounter, null);
    }

    protected JnksIotMsgProcessingCtx(int ruleNodeExecCounter, LinkedList<JnksIotMsgProcessingStackItem> stack) {
        this.ruleNodeExecCounter = new AtomicInteger(ruleNodeExecCounter);
        this.stack = stack;
    }

    public int getAndIncrementRuleNodeCounter() {
        return ruleNodeExecCounter.getAndIncrement();
    }

    public JnksIotMsgProcessingCtx copy() {
        if (stack == null || stack.isEmpty()) {
            return new JnksIotMsgProcessingCtx(ruleNodeExecCounter.get());
        } else {
            return new JnksIotMsgProcessingCtx(ruleNodeExecCounter.get(), new LinkedList<>(stack));
        }
    }

    public void push(RuleChainId ruleChainId, RuleNodeId ruleNodeId) {
        if (stack == null) {
            stack = new LinkedList<>();
        }
        stack.add(new JnksIotMsgProcessingStackItem(ruleChainId, ruleNodeId));
    }

    public JnksIotMsgProcessingStackItem pop() {
        if (stack == null || stack.isEmpty()) {
            return null;
        }
        return stack.removeLast();
    }

    public static JnksIotMsgProcessingCtx fromProto(MsgProtos.JnksIotMsgProcessingCtxProto ctx) {
        int ruleNodeExecCounter = ctx.getRuleNodeExecCounter();
        if (ctx.getStackCount() > 0) {
            LinkedList<JnksIotMsgProcessingStackItem> stack = new LinkedList<>();
            for (MsgProtos.JnksIotMsgProcessingStackItemProto item : ctx.getStackList()) {
                stack.add(JnksIotMsgProcessingStackItem.fromProto(item));
            }
            return new JnksIotMsgProcessingCtx(ruleNodeExecCounter, stack);
        } else {
            return new JnksIotMsgProcessingCtx(ruleNodeExecCounter);
        }
    }

    public MsgProtos.JnksIotMsgProcessingCtxProto toProto() {
        var ctxBuilder = MsgProtos.JnksIotMsgProcessingCtxProto.newBuilder();
        ctxBuilder.setRuleNodeExecCounter(ruleNodeExecCounter.get());
        if (stack != null) {
            for (JnksIotMsgProcessingStackItem item : stack) {
                ctxBuilder.addStack(item.toProto());
            }
        }
        return ctxBuilder.build();
    }
}
