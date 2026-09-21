package com.jnks.iot.server.service.entitiy.customer;

import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.service.entitiy.SimpleJnksIotEntityService;

/**
 * 客户业务层契约，继承通用保存/删除。
 * <p>
 * 由 CustomerController 调用；实现类委托 Customer DAO 并写审计日志。
 */
public interface JnksIotCustomerService extends SimpleJnksIotEntityService<Customer> {

}
