/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.util;

import org.thingsboard.server.common.data.transport.mqtt.MqttUplinkTopicMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * 按档案上行主题列表顺序匹配设备 PUBLISH 主题（先匹配先赢）。
 */
public final class MqttUplinkTopicMatcher {

    private final List<Entry> entries;

    public MqttUplinkTopicMatcher(List<MqttUplinkTopicMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            this.entries = List.of();
            return;
        }
        List<Entry> compiled = new ArrayList<>(mappings.size());
        for (MqttUplinkTopicMapping mapping : mappings) {
            compiled.add(new Entry(MqttTopicFilterFactory.toFilter(mapping.getTopic()), mapping));
        }
        this.entries = List.copyOf(compiled);
    }

    public MqttUplinkTopicMapping find(String topic) {
        if (topic == null || entries.isEmpty()) {
            return null;
        }
        for (Entry entry : entries) {
            if (entry.filter.filter(topic)) {
                return entry.mapping;
            }
        }
        return null;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    private record Entry(MqttTopicFilter filter, MqttUplinkTopicMapping mapping) {
    }
}
