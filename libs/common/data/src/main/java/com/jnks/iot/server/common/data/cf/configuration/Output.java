package com.jnks.iot.server.common.data.cf.configuration;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import com.jnks.iot.server.common.data.AttributeScope;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Output {

    private String name;
    private OutputType type;
    private AttributeScope scope;
    private Integer decimalsByDefault;

}
