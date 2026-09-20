package com.jnks.iot.server.dao.sql.event;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.dao.config.DedicatedEventsDataSource;
import com.jnks.iot.server.dao.sql.ScheduledLogExecutorComponent;
import com.jnks.iot.server.dao.sqlts.insert.sql.DedicatedEventsSqlPartitioningRepository;
import com.jnks.iot.server.dao.util.SqlDao;

@DedicatedEventsDataSource
@Component
@SqlDao
public class DedicatedJpaEventDao extends JpaBaseEventDao {

    public DedicatedJpaEventDao(EventPartitionConfiguration partitionConfiguration,
                                DedicatedEventsSqlPartitioningRepository partitioningRepository,
                                LifecycleEventRepository lcEventRepository,
                                StatisticsEventRepository statsEventRepository,
                                ErrorEventRepository errorEventRepository,
                                DedicatedEventInsertRepository eventInsertRepository,
                                RuleNodeDebugEventRepository ruleNodeDebugEventRepository,
                                RuleChainDebugEventRepository ruleChainDebugEventRepository,
                                ScheduledLogExecutorComponent logExecutor,
                                StatsFactory statsFactory,
                                CalculatedFieldDebugEventRepository cfDebugEventRepository) {
        super(partitionConfiguration, partitioningRepository, lcEventRepository, statsEventRepository,
                errorEventRepository, eventInsertRepository, ruleNodeDebugEventRepository,
                ruleChainDebugEventRepository, logExecutor, statsFactory, cfDebugEventRepository);
    }

}
