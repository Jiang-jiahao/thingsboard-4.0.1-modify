package com.jnks.iot.rule.engine.math;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.slf4j.Slf4j;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.springframework.util.ConcurrentReferenceHashMap;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.SemaphoreWithJnksIotMsgQueue;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.DoubleDataEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.LongDataEntry;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.jnks.iot.common.util.ExpressionFunctionsUtil.userDefinedFunctions;
import static com.jnks.iot.rule.engine.math.JnksIotMathArgumentType.CONSTANT;

@SuppressWarnings("UnstableApiUsage")
@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "math function",
        configClazz = JnksIotMathNodeConfiguration.class,
        nodeDescription = "Apply math function and save the result into the message and/or database",
        nodeDetails = "Supports math operations like: ADD, SUB, MULT, DIV, etc and functions: SIN, COS, TAN, SEC, etc. " +
                "Use 'CUSTOM' operation to specify complex math expressions." +
                "<br/><br/>" +
                "You may use constant, message field, metadata field, attribute, and latest time-series as an arguments values. " +
                "The result of the function may be also stored to message field, metadata field, attribute or time-series value." +
                "<br/><br/>" +
                "Primary use case for this rule node is to take one or more values from the database and modify them based on data from the message. " +
                "For example, you may increase `totalWaterConsumption` based on the `deltaWaterConsumption` reported by device." +
                "<br/><br/>" +
                "Alternative use case is the replacement of simple JS `script` nodes with more light-weight and performant implementation. " +
                "For example, you may transform Fahrenheit to Celsius (C = (F - 32) / 1.8) using CUSTOM operation and expression: (x - 32) / 1.8)." +
                "<br/><br/>" +
                "The execution is synchronized in scope of message originator (e.g. device) and server node. " +
                "If you have rule nodes in different rule chains, they will process messages from the same originator synchronously in the scope of the server node.",
        configDirective = "jnksIotActionNodeMathFunctionConfig",
        icon = "calculate"

)
public class JnksIotMathNode implements JnksIotNode {

    private static final ConcurrentMap<EntityId, SemaphoreWithJnksIotMsgQueue> locks = new ConcurrentReferenceHashMap<>(16, ConcurrentReferenceHashMap.ReferenceType.WEAK);
    private final ThreadLocal<Expression> customExpression = new ThreadLocal<>();
    private JnksIotMathNodeConfiguration config;
    private boolean msgBodyToJsonConversionRequired;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMathNodeConfiguration.class);
        var operation = config.getOperation();
        var argsCount = config.getArguments().size();
        if (argsCount < operation.getMinArgs() || argsCount > operation.getMaxArgs()) {
            throw new RuntimeException("Args count: " + argsCount + " does not match operation: " + operation.name());
        }
        if (JnksIotRuleNodeMathFunctionType.CUSTOM.equals(operation)) {
            if (StringUtils.isBlank(config.getCustomFunction())) {
                throw new RuntimeException("Custom function is blank!");
            } else if (config.getCustomFunction().length() > 256) {
                throw new RuntimeException("Custom function is too complex (length > 256)!");
            }
        }
        msgBodyToJsonConversionRequired = config.getArguments().stream().anyMatch(arg -> JnksIotMathArgumentType.MESSAGE_BODY.equals(arg.getType()));
        msgBodyToJsonConversionRequired = msgBodyToJsonConversionRequired || JnksIotMathArgumentType.MESSAGE_BODY.equals(config.getResult().getType());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        locks.computeIfAbsent(msg.getOriginator(), SemaphoreWithJnksIotMsgQueue::new)
                .addToQueueAndTryProcess(msg, ctx, this::processMsgAsync);
    }

    ListenableFuture<JnksIotMsg> processMsgAsync(JnksIotContext ctx, JnksIotMsg msg) {
        var arguments = config.getArguments();
        Optional<ObjectNode> msgBodyOpt = convertMsgBodyIfRequired(msg);
        var argumentValues = Futures.allAsList(arguments.stream()
                .map(arg -> resolveArguments(ctx, msg, msgBodyOpt, arg)).collect(Collectors.toList()));
        ListenableFuture<JnksIotMsg> resultMsgFuture = Futures.transformAsync(argumentValues, args ->
                updateMsgAndDb(ctx, msg, msgBodyOpt, calculateResult(args)), ctx.getDbCallbackExecutor());
        return resultMsgFuture;
    }

    private ListenableFuture<JnksIotMsg> updateMsgAndDb(JnksIotContext ctx, JnksIotMsg msg, Optional<ObjectNode> msgBodyOpt, double result) {
        JnksIotMathResult mathResultDef = config.getResult();
        String mathResultKey = getKeyFromTemplate(msg, mathResultDef.getType(), mathResultDef.getKey());
        switch (mathResultDef.getType()) {
            case MESSAGE_BODY:
                return Futures.immediateFuture(addToBody(msg, mathResultDef, mathResultKey, msgBodyOpt, result));
            case MESSAGE_METADATA:
                return Futures.immediateFuture(addToMeta(msg, mathResultDef, mathResultKey, result));
            case ATTRIBUTE:
                ListenableFuture<Void> attrSave = saveAttribute(ctx, msg, result, mathResultDef);
                return Futures.transform(attrSave, attr -> addToBodyAndMeta(msg, msgBodyOpt, result, mathResultDef, mathResultKey), ctx.getDbCallbackExecutor());
            case TIME_SERIES:
                ListenableFuture<Void> tsSave = saveTimeSeries(ctx, msg, result, mathResultDef);
                return Futures.transform(tsSave, ts -> addToBodyAndMeta(msg, msgBodyOpt, result, mathResultDef, mathResultKey), ctx.getDbCallbackExecutor());
            default:
                throw new RuntimeException("Result type is not supported: " + mathResultDef.getType() + "!");
        }
    }

    private ListenableFuture<Void> saveTimeSeries(JnksIotContext ctx, JnksIotMsg msg, double result, JnksIotMathResult mathResultDef) {
        final BasicTsKvEntry basicTsKvEntry = new BasicTsKvEntry(System.currentTimeMillis(), new DoubleDataEntry(mathResultDef.getKey(), result));
        SettableFuture<Void> future = SettableFuture.create();
        ctx.getTelemetryService().saveTimeseries(TimeseriesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .entityId(msg.getOriginator())
                .entry(basicTsKvEntry)
                .future(future)
                .build());
        return future;
    }

    private ListenableFuture<Void> saveAttribute(JnksIotContext ctx, JnksIotMsg msg, double result, JnksIotMathResult mathResultDef) {
        AttributeScope attributeScope = getAttributeScope(mathResultDef.getAttributeScope());
        KvEntry kvEntry;
        if (isIntegerResult(mathResultDef, config.getOperation())) {
            var value = toIntValue(result);
            kvEntry = new LongDataEntry(mathResultDef.getKey(), value);
        } else {
            var value = toDoubleValue(mathResultDef, result);
            kvEntry = new DoubleDataEntry(mathResultDef.getKey(), value);
        }
        SettableFuture<Void> future = SettableFuture.create();
        ctx.getTelemetryService().saveAttributes(AttributesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .entityId(msg.getOriginator())
                .scope(attributeScope)
                .entry(kvEntry)
                .future(future)
                .build());
        return future;
    }

    private boolean isIntegerResult(JnksIotMathResult mathResultDef, JnksIotRuleNodeMathFunctionType function) {
        return function.isIntegerResult() || mathResultDef.getResultValuePrecision() == 0;
    }

    private long toIntValue(double value) {
        return (long) value;
    }

    private double toDoubleValue(JnksIotMathResult mathResultDef, double value) {
        return BigDecimal.valueOf(value).setScale(mathResultDef.getResultValuePrecision(), RoundingMode.HALF_UP).doubleValue();
    }

    private Optional<ObjectNode> convertMsgBodyIfRequired(JnksIotMsg msg) {
        Optional<ObjectNode> msgBodyOpt;
        if (msgBodyToJsonConversionRequired) {
            var jsonNode = JacksonUtil.toJsonNode(msg.getData());
            if (jsonNode.isObject()) {
                msgBodyOpt = Optional.of((ObjectNode) jsonNode);
            } else {
                throw new RuntimeException("Message body is not a JSON object!");
            }
        } else {
            msgBodyOpt = Optional.empty();
        }
        return msgBodyOpt;
    }

    private JnksIotMsg addToBodyAndMeta(JnksIotMsg msg, Optional<ObjectNode> msgBodyOpt, double result, JnksIotMathResult mathResultDef, String mathResultKey) {
        JnksIotMsg tmpMsg = msg;
        if (mathResultDef.isAddToBody()) {
            tmpMsg = addToBody(tmpMsg, mathResultDef, mathResultKey, msgBodyOpt, result);
        }
        if (mathResultDef.isAddToMetadata()) {
            tmpMsg = addToMeta(tmpMsg, mathResultDef, mathResultKey, result);
        }
        return tmpMsg;
    }

    private JnksIotMsg addToBody(JnksIotMsg msg, JnksIotMathResult mathResultDef, String mathResultKey, Optional<ObjectNode> msgBodyOpt, double result) {
        ObjectNode body = msgBodyOpt.get();
        if (isIntegerResult(mathResultDef, config.getOperation())) {
            body.put(mathResultKey, toIntValue(result));
        } else {
            body.put(mathResultKey, toDoubleValue(mathResultDef, result));
        }
        return msg.transform()
                .data(JacksonUtil.toString(body))
                .build();
    }

    private JnksIotMsg addToMeta(JnksIotMsg msg, JnksIotMathResult mathResultDef, String mathResultKey, double result) {
        var md = msg.getMetaData();
        if (isIntegerResult(mathResultDef, config.getOperation())) {
            md.putValue(mathResultKey, Long.toString(toIntValue(result)));
        } else {
            md.putValue(mathResultKey, Double.toString(toDoubleValue(mathResultDef, result)));
        }
        return msg.transform()
                .metaData(md)
                .build();
    }

    private double calculateResult(List<JnksIotMathArgumentValue> args) {
        switch (config.getOperation()) {
            case ADD:
                return apply(args.get(0), args.get(1), Double::sum);
            case SUB:
                return apply(args.get(0), args.get(1), (a, b) -> a - b);
            case MULT:
                return apply(args.get(0), args.get(1), (a, b) -> a * b);
            case DIV:
                return apply(args.get(0), args.get(1), (a, b) -> a / b);
            case SIN:
                return apply(args.get(0), Math::sin);
            case SINH:
                return apply(args.get(0), Math::sinh);
            case COS:
                return apply(args.get(0), Math::cos);
            case COSH:
                return apply(args.get(0), Math::cosh);
            case TAN:
                return apply(args.get(0), Math::tan);
            case TANH:
                return apply(args.get(0), Math::tanh);
            case ACOS:
                return apply(args.get(0), Math::acos);
            case ASIN:
                return apply(args.get(0), Math::asin);
            case ATAN:
                return apply(args.get(0), Math::atan);
            case ATAN2:
                return apply(args.get(0), args.get(1), Math::atan2);
            case EXP:
                return apply(args.get(0), Math::exp);
            case EXPM1:
                return apply(args.get(0), Math::expm1);
            case SQRT:
                return apply(args.get(0), Math::sqrt);
            case CBRT:
                return apply(args.get(0), Math::cbrt);
            case GET_EXP:
                return apply(args.get(0), (x) -> (double) Math.getExponent(x));
            case HYPOT:
                return apply(args.get(0), args.get(1), Math::hypot);
            case LOG:
                return apply(args.get(0), Math::log);
            case LOG10:
                return apply(args.get(0), Math::log10);
            case LOG1P:
                return apply(args.get(0), Math::log1p);
            case CEIL:
                return apply(args.get(0), Math::ceil);
            case FLOOR:
                return apply(args.get(0), Math::floor);
            case FLOOR_DIV:
                return apply(args.get(0), args.get(1), (a, b) -> (double) Math.floorDiv(a.longValue(), b.longValue()));
            case FLOOR_MOD:
                return apply(args.get(0), args.get(1), (a, b) -> (double) Math.floorMod(a.longValue(), b.longValue()));
            case ABS:
                return apply(args.get(0), Math::abs);
            case MIN:
                return apply(args.get(0), args.get(1), Math::min);
            case MAX:
                return apply(args.get(0), args.get(1), Math::max);
            case POW:
                return apply(args.get(0), args.get(1), Math::pow);
            case SIGNUM:
                return apply(args.get(0), Math::signum);
            case RAD:
                return apply(args.get(0), Math::toRadians);
            case DEG:
                return apply(args.get(0), Math::toDegrees);
            case CUSTOM:
                var expr = customExpression.get();
                if (expr == null) {
                    expr = new ExpressionBuilder(config.getCustomFunction())
                            .functions(userDefinedFunctions)
                            .implicitMultiplication(true)
                            .variables(config.getArguments().stream().map(JnksIotMathArgument::getName).collect(Collectors.toSet()))
                            .build();
                    customExpression.set(expr);
                }
                for (int i = 0; i < config.getArguments().size(); i++) {
                    expr.setVariable(config.getArguments().get(i).getName(), args.get(i).getValue());
                }
                return expr.evaluate();
            default:
                throw new RuntimeException("Not supported operation: " + config.getOperation());
        }
    }

    private double apply(JnksIotMathArgumentValue arg, Function<Double, Double> function) {
        return function.apply(arg.getValue());
    }

    private double apply(JnksIotMathArgumentValue arg1, JnksIotMathArgumentValue arg2, BiFunction<Double, Double, Double> function) {
        return function.apply(arg1.getValue(), arg2.getValue());
    }

    ListenableFuture<JnksIotMathArgumentValue> resolveArguments(JnksIotContext ctx, JnksIotMsg msg, Optional<ObjectNode> msgBodyOpt, JnksIotMathArgument arg) {
        String argKey = getKeyFromTemplate(msg, arg.getType(), arg.getKey());
        switch (arg.getType()) {
            case CONSTANT:
                return Futures.immediateFuture(JnksIotMathArgumentValue.constant(arg));
            case MESSAGE_BODY:
                return Futures.immediateFuture(JnksIotMathArgumentValue.fromMessageBody(arg, argKey, msgBodyOpt));
            case MESSAGE_METADATA:
                return Futures.immediateFuture(JnksIotMathArgumentValue.fromMessageMetadata(arg, argKey, msg.getMetaData()));
            case ATTRIBUTE:
                AttributeScope scope = getAttributeScope(arg.getAttributeScope());
                return Futures.transform(ctx.getAttributesService().find(ctx.getTenantId(), msg.getOriginator(), scope, argKey),
                        opt -> getJnksIotMathArgumentValue(arg, opt, "Attribute: " + argKey + " with scope: " + scope + " not found for entity: " + msg.getOriginator())
                        , MoreExecutors.directExecutor());
            case TIME_SERIES:
                return Futures.transform(ctx.getTimeseriesService().findLatest(ctx.getTenantId(), msg.getOriginator(), argKey),
                        opt -> getJnksIotMathArgumentValue(arg, opt, "Time-series: " + argKey + " not found for entity: " + msg.getOriginator())
                        , MoreExecutors.directExecutor());
            default:
                throw new RuntimeException("Unsupported argument type: " + arg.getType() + "!");
        }

    }

    private String getKeyFromTemplate(JnksIotMsg msg, JnksIotMathArgumentType type, String keyPattern) {
        return CONSTANT.equals(type) ? keyPattern : JnksIotNodeUtils.processPattern(keyPattern, msg);
    }

    private AttributeScope getAttributeScope(String attrScope) {
        return StringUtils.isEmpty(attrScope) ? AttributeScope.SERVER_SCOPE : AttributeScope.valueOf(attrScope);
    }

    private JnksIotMathArgumentValue getJnksIotMathArgumentValue(JnksIotMathArgument arg, Optional<? extends KvEntry> kvOpt, String error) {
        if (kvOpt != null && kvOpt.isPresent()) {
            var kv = kvOpt.get();
            switch (kv.getDataType()) {
                case LONG:
                    return JnksIotMathArgumentValue.fromLong(kv.getLongValue().get());
                case DOUBLE:
                    return JnksIotMathArgumentValue.fromDouble(kv.getDoubleValue().get());
                default:
                    return JnksIotMathArgumentValue.fromString(kv.getValueAsString());
            }
        } else {
            if (arg.getDefaultValue() != null) {
                return JnksIotMathArgumentValue.fromDouble(arg.getDefaultValue());
            } else {
                throw new RuntimeException(error);
            }
        }
    }

}
