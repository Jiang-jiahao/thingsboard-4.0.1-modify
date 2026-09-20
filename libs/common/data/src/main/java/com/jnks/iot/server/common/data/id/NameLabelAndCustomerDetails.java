package com.jnks.iot.server.common.data.id;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class NameLabelAndCustomerDetails {
    private final String name;
    private final String label;
    private final CustomerId customerId;
}
