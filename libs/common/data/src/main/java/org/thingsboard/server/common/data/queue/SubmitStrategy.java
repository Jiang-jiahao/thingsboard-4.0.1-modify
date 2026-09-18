package org.thingsboard.server.common.data.queue;

import lombok.Data;

import java.io.Serializable;

@Data
public class SubmitStrategy implements Serializable {

    private static final long serialVersionUID = 2938457983298400L;

    private SubmitStrategyType type;
    private int batchSize;
}