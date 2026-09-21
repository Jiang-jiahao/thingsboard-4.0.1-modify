package com.jnks.iot.rule.engine.math;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JnksIotMathArgument {

    private String name;
    private JnksIotMathArgumentType type;
    private String key;
    private String attributeScope;
    private Double defaultValue;

    public JnksIotMathArgument(JnksIotMathArgumentType type, String key) {
       this(key, type, key, null, null);
    }

    public JnksIotMathArgument(String name, JnksIotMathArgumentType type, String key) {
       this(name, type, key, null, null);
    }

}
