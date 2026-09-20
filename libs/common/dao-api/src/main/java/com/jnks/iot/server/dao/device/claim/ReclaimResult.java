package com.jnks.iot.server.dao.device.claim;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.data.Customer;

@Data
@AllArgsConstructor
public class ReclaimResult {
    Customer unassignedCustomer;
}
