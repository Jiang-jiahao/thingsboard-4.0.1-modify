package com.jnks.iot.server.dao.sql.cf;

import org.springframework.data.domain.Pageable;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.page.PageData;

public interface NativeCalculatedFieldRepository {

    PageData<CalculatedField> findCalculatedFields(Pageable pageable);

    PageData<CalculatedFieldLink> findCalculatedFieldLinks(Pageable pageable);

}
