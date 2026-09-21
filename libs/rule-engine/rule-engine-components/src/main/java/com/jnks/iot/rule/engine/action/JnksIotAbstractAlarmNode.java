package com.jnks.iot.rule.engine.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import static com.jnks.iot.common.util.DonAsynchron.withCallback;


@Slf4j
public abstract class JnksIotAbstractAlarmNode<C extends JnksIotAbstractAlarmNodeConfiguration> implements JnksIotNode {

    static final String PREV_ALARM_DETAILS = "prevAlarmDetails";

    protected C config;
    private ScriptEngine scriptEngine;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = loadAlarmNodeConfig(configuration);
        scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getAlarmDetailsBuildTbel() : config.getAlarmDetailsBuildJs());
    }

    protected abstract C loadAlarmNodeConfig(JnksIotNodeConfiguration configuration) throws JnksIotNodeException;

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        withCallback(processAlarm(ctx, msg),
                alarmResult -> {
                    if (alarmResult.alarm == null) {
                        ctx.tellNext(msg, JnksIotNodeConnectionType.FALSE);
                    } else if (alarmResult.isCreated) {
                        tellNext(ctx, msg, alarmResult, JnksIotMsgType.ENTITY_CREATED, "Created");
                    } else if (alarmResult.isUpdated || alarmResult.isSeverityUpdated) {
                        tellNext(ctx, msg, alarmResult, JnksIotMsgType.ENTITY_UPDATED, "Updated");
                    } else if (alarmResult.isCleared) {
                        tellNext(ctx, msg, alarmResult, JnksIotMsgType.ALARM_CLEAR, "Cleared");
                    } else {
                        ctx.tellSuccess(msg);
                    }
                },
                t -> ctx.tellFailure(msg, t), ctx.getDbCallbackExecutor());
    }

    protected abstract ListenableFuture<JnksIotAlarmResult> processAlarm(JnksIotContext ctx, JnksIotMsg msg);

    protected ListenableFuture<JsonNode> buildAlarmDetails(JnksIotMsg msg, JsonNode previousDetails) {
        try {
            JnksIotMsg dummyMsg = msg;
            if (previousDetails != null) {
                JnksIotMsgMetaData metaData = msg.getMetaData().copy();
                metaData.putValue(PREV_ALARM_DETAILS, JacksonUtil.toString(previousDetails));
                dummyMsg = msg.transform()
                        .metaData(metaData)
                        .build();
            }
            return scriptEngine.executeJsonAsync(dummyMsg);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    public static JnksIotMsg toAlarmMsg(JnksIotContext ctx, JnksIotAlarmResult alarmResult, JnksIotMsg originalMsg) {
        JsonNode jsonNodes = JacksonUtil.valueToTree(alarmResult.alarm);
        String data = jsonNodes.toString();
        JnksIotMsgMetaData metaData = originalMsg.getMetaData().copy();
        if (alarmResult.isCreated) {
            metaData.putValue(DataConstants.IS_NEW_ALARM, Boolean.TRUE.toString());
        } else if (alarmResult.isUpdated || alarmResult.isSeverityUpdated) {
            metaData.putValue(DataConstants.IS_EXISTING_ALARM, Boolean.TRUE.toString());
        } else if (alarmResult.isCleared) {
            metaData.putValue(DataConstants.IS_CLEARED_ALARM, Boolean.TRUE.toString());
        }
        return ctx.transformMsg(originalMsg, JnksIotMsgType.ALARM, originalMsg.getOriginator(), metaData, data);
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }

    private void tellNext(JnksIotContext ctx, JnksIotMsg msg, JnksIotAlarmResult alarmResult, JnksIotMsgType actionMsgType, String alarmAction) {
        ctx.enqueue(ctx.alarmActionMsg(alarmResult.alarm, ctx.getSelfId(), actionMsgType),
                () -> ctx.tellNext(toAlarmMsg(ctx, alarmResult, msg), alarmAction),
                throwable -> ctx.tellFailure(toAlarmMsg(ctx, alarmResult, msg), throwable));
    }
}
