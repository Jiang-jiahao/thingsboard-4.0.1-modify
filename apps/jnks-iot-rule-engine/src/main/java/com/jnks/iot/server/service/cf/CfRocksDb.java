package com.jnks.iot.server.service.cf;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotRocksDb;

@Component
@ConditionalOnExpression("'${queue.type:null}'=='in-memory'")
public class CfRocksDb extends JnksIotRocksDb {

    public CfRocksDb(@Value("${queue.calculated_fields.rocks_db_path:${user.home}/.rocksdb/cf_states}") String path) {
        super(path, new Options().setCreateIfMissing(true), new WriteOptions().setSync(true));
    }

    @PostConstruct
    @Override
    public void init() {
        super.init();
    }

    @PreDestroy
    @Override
    public void close() {
        super.close();
    }

}
