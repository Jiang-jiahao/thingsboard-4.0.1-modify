package com.jnks.iot.rule.engine.math;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.List;

@Data
public class JnksIotMathNodeConfiguration implements NodeConfiguration<JnksIotMathNodeConfiguration> {

    private JnksIotRuleNodeMathFunctionType operation;
    private List<JnksIotMathArgument> arguments;
    private String customFunction;
    private JnksIotMathResult result;

    @Override
    public JnksIotMathNodeConfiguration defaultConfiguration() {
        JnksIotMathNodeConfiguration configuration = new JnksIotMathNodeConfiguration();
        configuration.setOperation(JnksIotRuleNodeMathFunctionType.CUSTOM);
        configuration.setCustomFunction("(x - 32) / 1.8");
        configuration.setArguments(List.of(new JnksIotMathArgument("x", JnksIotMathArgumentType.MESSAGE_BODY, "temperature")));
        configuration.setResult(new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "temperatureCelsius", 2, false, false, null));
        return configuration;
    }
}