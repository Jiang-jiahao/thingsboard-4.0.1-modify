package com.jnks.iot.rule.engine.action;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.HashMap;
import java.util.Map;

@Data
public class JnksIotSaveToCustomCassandraTableNodeConfiguration implements NodeConfiguration<JnksIotSaveToCustomCassandraTableNodeConfiguration> {


    private String tableName;
    private Map<String, String> fieldsMapping;
    private int defaultTtl;


    @Override
    public JnksIotSaveToCustomCassandraTableNodeConfiguration defaultConfiguration() {
        JnksIotSaveToCustomCassandraTableNodeConfiguration configuration = new JnksIotSaveToCustomCassandraTableNodeConfiguration();
        configuration.setDefaultTtl(0);
        configuration.setTableName("");
        Map<String, String> map = new HashMap<>();
        map.put("", "");
        configuration.setFieldsMapping(map);
        return configuration;
    }
}
