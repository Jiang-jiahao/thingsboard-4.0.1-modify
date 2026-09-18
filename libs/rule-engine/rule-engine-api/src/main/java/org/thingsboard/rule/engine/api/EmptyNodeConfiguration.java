package org.thingsboard.rule.engine.api;

import lombok.Data;

@Data
public class EmptyNodeConfiguration implements NodeConfiguration<EmptyNodeConfiguration> {

    private int version;

    @Override
    public EmptyNodeConfiguration defaultConfiguration() {
        return new EmptyNodeConfiguration();
    }
}
