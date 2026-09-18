package org.thingsboard.server.service.entitiy.customer;

import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

/**
 * 客户业务层契约，继承通用保存/删除。
 * <p>
 * 由 CustomerController 调用；实现类委托 Customer DAO 并写审计日志。
 */
public interface TbCustomerService extends SimpleTbEntityService<Customer> {

}
