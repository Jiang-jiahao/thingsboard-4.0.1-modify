package com.jnks.iot.server.common.data.util;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JnksIotPair<S, T> {
    private S first;
    private T second;

    public static <S, T> JnksIotPair<S, T> of(S first, T second) {
        return new JnksIotPair<>(first, second);
    }
}
