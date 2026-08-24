/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsRequest;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.query.AlarmCountQuery;
import org.thingsboard.server.common.data.query.AlarmData;
import org.thingsboard.server.common.data.query.AlarmDataQuery;
import org.thingsboard.server.common.data.query.EntityCountQuery;
import org.thingsboard.server.common.data.query.EntityData;
import org.thingsboard.server.common.data.query.EntityDataPageLink;
import org.thingsboard.server.common.data.query.EntityDataQuery;
import org.thingsboard.server.common.msg.edqs.EdqsApiService;
import org.thingsboard.server.common.msg.edqs.EdqsService;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.query.EntityQueryService;
import org.thingsboard.server.service.security.permission.Operation;

import static org.thingsboard.server.controller.ControllerConstants.ALARM_DATA_QUERY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ATTRIBUTES_SCOPE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_COUNT_QUERY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_DATA_QUERY_DESCRIPTION;

/**
 * 实体 / 告警查询 REST 入口，以及 EDQS 系统级管理接口。
 * <p>
 * 仅在 {@link TbCoreComponent}（Core / Monolith）中生效。本类本身不决定查
 * PostgreSQL 还是 EDQS：业务查询一律交给 {@link EntityQueryService}，后者再经
 * {@code EntityService}（{@code BaseEntityService}）按
 * {@link EdqsApiService#isEnabled()} 分流。
 * <p>
 * <b>接口分两组：</b>
 * <ol>
 *   <li><b>业务查询（租户/客户）：</b>{@code /entitiesQuery/*}、{@code /alarmsQuery/*}。
 *       仪表盘、实体表、告警表等 UI 的过滤/分页都走这里。
 *       实体 count/find 在 EDQS API 打开后会打到内存索引；告警 count 仍走 DB，
 *       告警 find 会先按实体过滤（这段可能走 EDQS）再查告警表。</li>
 *   <li><b>EDQS 管理（仅 SYS_ADMIN）：</b>手动触发全量同步 / 开关查询 API，
 *       以及查看本节点 {@code apiEnabled}。对应 {@link EdqsService#processSystemRequest}
 *       那条「广播 → 抢锁 sync / 各节点 setEnabled」集群通道。</li>
 * </ol>
 *
 * @see EntityQueryService
 * @see EdqsService
 * @see EdqsApiService
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
public class EntityQueryController extends BaseController {

    /** 实体 / 告警查询业务实现；动态值解析、告警与实体数据拼装都在这一层。 */
    @Autowired
    private EntityQueryService entityQueryService;
    /**
     * EDQS <strong>写路径 / 集群协作</strong>入口。
     * 本控制器只用它下发系统请求（全量 sync、广播 apiEnabled），不直接写 events。
     */
    @Autowired
    private EdqsService edqsService;
    /**
     * 本节点 EDQS <strong>查询 API 开关</strong>。
     * {@link #isEdqsApiEnabled()} 读的就是它；真正的实体查询分流发生在 DAO 层。
     */
    @Autowired
    private EdqsApiService edqsApiService;

    /**
     * {@link #findEntityTimeseriesAndAttributesKeysByQuery} 扫实体的上限。
     * 该接口只为 UI 自动补全收集 key，不需要翻完全部实体。
     */
    private static final int MAX_PAGE_SIZE = 100;

    /**
     * 按过滤条件统计实体数量。
     * <p>
     * 允许 SYS_ADMIN（跨租户运维场景也需要 count）。下游
     * {@code BaseEntityService#countEntitiesByQuery}：EDQS 已启用、查询合法且
     * 非系统租户时走 EDQS，否则走 SQL。
     */
    @ApiOperation(value = "Count Entities by Query", notes = ENTITY_COUNT_QUERY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entitiesQuery/count", method = RequestMethod.POST)
    @ResponseBody
    public long countEntitiesByQuery(
            @Parameter(description = "A JSON value representing the entity count query. See API call notes above for more details.")
            @RequestBody EntityCountQuery query) throws ThingsboardException {
        checkNotNull(query);
        return this.entityQueryService.countEntitiesByQuery(getCurrentUser(), query);
    }

    /**
     * 按过滤、排序、分页查出实体及所选字段/最新遥测/属性。
     * <p>
     * 不含 SYS_ADMIN：结果带租户数据，只开放给租户管理员和客户用户。
     * {@link EntityQueryService} 会先解析 KeyFilter 里的动态值（当前租户/客户/用户属性），
     * 再交给 {@code EntityService}；EDQS 打开时走内存查询，否则走 SQL（部分查询还会做
     * 「先筛 ID 再按 ID 拉字段」的优化）。
     */
    @ApiOperation(value = "Find Entity Data by Query", notes = ENTITY_DATA_QUERY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entitiesQuery/find", method = RequestMethod.POST)
    @ResponseBody
    public PageData<EntityData> findEntityDataByQuery(
            @Parameter(description = "A JSON value representing the entity data query. See API call notes above for more details.")
            @RequestBody EntityDataQuery query) throws ThingsboardException {
        checkNotNull(query);
        return this.entityQueryService.findEntityDataByQuery(getCurrentUser(), query);
    }

    /**
     * 按实体过滤 + 告警条件分页查告警，并把实体最新字段拼进 {@link AlarmData}。
     * <p>
     * 若指定了处理人 {@code assigneeId}，先校验当前用户对该用户有 READ 权限，
     * 避免用告警查询越权看别人。
     * <p>
     * 实现上先把告警查询收成实体查询（最多
     * {@code server.ws.max_entities_per_alarm_subscription} 个实体），
     * 这段可能走 EDQS；告警行本身仍查告警表。
     */
    @ApiOperation(value = "Find Alarms by Query", notes = ALARM_DATA_QUERY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/alarmsQuery/find", method = RequestMethod.POST)
    @ResponseBody
    public PageData<AlarmData> findAlarmDataByQuery(
            @Parameter(description = "A JSON value representing the alarm data query. See API call notes above for more details.")
            @RequestBody AlarmDataQuery query) throws ThingsboardException {
        checkNotNull(query);
        checkNotNull(query.getPageLink());
        UserId assigneeId = query.getPageLink().getAssigneeId();
        if (assigneeId != null) {
            checkUserId(assigneeId, Operation.READ);
        }
        return this.entityQueryService.findAlarmDataByQuery(getCurrentUser(), query);
    }

    /**
     * 按条件统计告警条数。告警计数走 {@code AlarmService} / DB，不经过 EDQS。
     * 指定处理人时同样先做 READ 校验。
     */
    @ApiOperation(value = "Count Alarms by Query (countAlarmsByQuery)", notes = "Returns the number of alarms that match the query definition.")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/alarmsQuery/count", method = RequestMethod.POST)
    @ResponseBody
    public long countAlarmsByQuery(@Parameter(description = "A JSON value representing the alarm count query.")
                                   @RequestBody AlarmCountQuery query) throws ThingsboardException {
        checkNotNull(query);
        UserId assigneeId = query.getAssigneeId();
        if (assigneeId != null) {
            checkUserId(assigneeId, Operation.READ);
        }
        return this.entityQueryService.countAlarmsByQuery(getCurrentUser(), query);
    }

    /**
     * 为 UI 提示收集「这批实体上出现过哪些时序 / 属性 key」。
     * <p>
     * 先按实体查询取出最多 {@link #MAX_PAGE_SIZE} 条（超过则截断，避免为补全扫全库），
     * 再异步去时序/属性库汇总去重 key。返回 {@link DeferredResult}，不占用 Tomcat 工作线程
     * 等 DB。{@code scope} 为空时三个属性范围都扫。
     */
    @ApiOperation(value = "Find Entity Keys by Query",
            notes = "Uses entity data query (see 'Find Entity Data by Query') to find first 100 entities. Then fetch and return all unique time-series and/or attribute keys. Used mostly for UI hints.")
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/entitiesQuery/find/keys", method = RequestMethod.POST)
    @ResponseBody
    public DeferredResult<ResponseEntity> findEntityTimeseriesAndAttributesKeysByQuery(
            @Parameter(description = "A JSON value representing the entity data query. See API call notes above for more details.")
            @RequestBody EntityDataQuery query,
            @Parameter(description = "Include all unique time-series keys to the result.")
            @RequestParam("timeseries") boolean isTimeseries,
            @Parameter(description = "Include all unique attribute keys to the result.")
            @RequestParam("attributes") boolean isAttributes,
            @Parameter(description = ATTRIBUTES_SCOPE_DESCRIPTION, schema = @Schema(allowableValues = {"SERVER_SCOPE", "SHARED_SCOPE", "CLIENT_SCOPE"}))
            @RequestParam(value = "scope", required = false) String scope) throws ThingsboardException {
        TenantId tenantId = getTenantId();
        checkNotNull(query);
        EntityDataPageLink pageLink = query.getPageLink();
        if (pageLink.getPageSize() > MAX_PAGE_SIZE) {
            pageLink.setPageSize(MAX_PAGE_SIZE);
        }
        return entityQueryService.getKeysByQuery(getCurrentUser(), tenantId, query, isTimeseries, isAttributes, scope);
    }

    /**
     * 系统管理员向「任意一台」Core 下发 EDQS 集群指令。
     * <p>
     * 请求体 {@link ToCoreEdqsRequest} 两类字段可并存：
     * <ul>
     *   <li>{@code syncRequest}：发起全量同步。本节点先把状态写成 REQUESTED，
     *       再广播给所有 Core，由抢到 {@code edqs_sync} 锁的那台真正扫库灌 events；</li>
     *   <li>{@code apiEnabled}：广播后每个 Core 改本机内存开关，决定实体查询是否走 EDQS。</li>
     * </ul>
     * 任意 Core 都可接收此 HTTP；真正执行方由广播 + 分布式锁决定，调用方不必是系统分区节点。
     */
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @PostMapping("/edqs/system/request")
    public void processSystemEdqsRequest(@RequestBody ToCoreEdqsRequest request) {
        edqsService.processSystemRequest(request);
    }

    /**
     * 返回<strong>本节点</strong> EDQS 查询 API 是否已打开。
     * <p>
     * {@code apiEnabled} 是进程内内存标志，不是集群共享配置；各 Core 可能短暂不一致。
     * 为 true 时 {@code BaseEntityService} 把实体 count/find 转发到 EDQS，否则继续查 PostgreSQL。
     */
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN')")
    @GetMapping("/edqs/enabled")
    public boolean isEdqsApiEnabled() {
        return edqsApiService.isEnabled();
    }

}
