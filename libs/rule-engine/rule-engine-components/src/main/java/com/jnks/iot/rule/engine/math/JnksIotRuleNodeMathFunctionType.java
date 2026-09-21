package com.jnks.iot.rule.engine.math;

import lombok.Getter;

public enum JnksIotRuleNodeMathFunctionType {

    ADD(2), SUB(2), MULT(2), DIV(2),
    SIN, SINH, COS, COSH, TAN, TANH, ACOS, ASIN, ATAN, ATAN2(2),
    EXP, EXPM1, SQRT, CBRT, GET_EXP(1, 1, true), HYPOT(2), LOG, LOG10, LOG1P,
    CEIL(1, 1, true), FLOOR(1, 1, true), FLOOR_DIV(2), FLOOR_MOD(2),
    ABS, MIN(2), MAX(2), POW(2), SIGNUM, RAD, DEG,

    CUSTOM(0, 16, false); //Custom function based on exp4j

    @Getter
    private final int minArgs;
    @Getter
    private final int maxArgs;
    @Getter
    private final boolean integerResult;

    JnksIotRuleNodeMathFunctionType() {
        this(1, 1, false);
    }

    JnksIotRuleNodeMathFunctionType(int args) {
        this(args, args, false);
    }

    JnksIotRuleNodeMathFunctionType(int minArgs, int maxArgs, boolean integerResult) {
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
        this.integerResult = integerResult;
    }

}
