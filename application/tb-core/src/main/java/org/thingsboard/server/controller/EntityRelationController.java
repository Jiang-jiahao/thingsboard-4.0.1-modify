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
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.EntityIdFactory;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationInfo;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.entity.relation.TbEntityRelationService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.Operation;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.thingsboard.server.controller.ControllerConstants.ENTITY_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.ENTITY_TYPE_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.RELATION_INFO_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.RELATION_TYPE_GROUP_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.RELATION_TYPE_PARAM_DESCRIPTION;

/**
 * 实体关系（Entity Relation）REST 入口。
 * <p>
 * 管理实体之间的有向关系（from / to、类型、类型组），以及按方向或复杂查询检索。
 * 仅在 {@link TbCoreComponent} 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。路径如 {@code /relation}、{@code /v2/relation}、
 * {@code /relations}、{@code /relations/info}。
 * <p>
 * <b>权限：</b>SYS_ADMIN、TENANT_ADMIN、CUSTOMER_USER；写关系时两端实体需 WRITE
 * （租户管理员把关系连到自己租户实体时有特例），读时两端需 READ，结果还会再按权限过滤。
 * <p>
 * <b>下游：</b>写/删走 {@link TbEntityRelationService}；查询走基类 {@code relationService}。
 *
 * @see TbEntityRelationService
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
public class EntityRelationController extends BaseController {

    private final TbEntityRelationService tbEntityRelationService;

    public static final String TO_TYPE = "toType";
    public static final String FROM_ID = "fromId";
    public static final String FROM_TYPE = "fromType";
    public static final String RELATION_TYPE = "relationType";
    public static final String TO_ID = "toId";

    private static final String SECURITY_CHECKS_ENTITIES_DESCRIPTION = "\n\nIf the user has the authority of 'System Administrator', the server checks that 'from' and 'to' entities are owned by the sysadmin. " +
            "If the user has the authority of 'Tenant Administrator', the server checks that 'from' and 'to' entities are owned by the same tenant. " +
            "If the user has the authority of 'Customer User', the server checks that the 'from' and 'to' entities are assigned to the same customer.";

    private static final String SECURITY_CHECKS_ENTITY_DESCRIPTION = "\n\nIf the user has the authority of 'System Administrator', the server checks that the entity is owned by the sysadmin. " +
            "If the user has the authority of 'Tenant Administrator', the server checks that the entity is owned by the same tenant. " +
            "If the user has the authority of 'Customer User', the server checks that the entity is assigned to the same customer.";

    /**
     * 创建或更新一条关系。唯一键为 from/to + 类型组 + 关系类型。无返回体。
     */
    @ApiOperation(value = "Create Relation (saveRelation)",
            notes = "Creates or updates a relation between two entities in the platform. " +
                    "Relations unique key is a combination of from/to entity id and relation type group and relation type. " +
                    SECURITY_CHECKS_ENTITIES_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relation", method = RequestMethod.POST)
    @ResponseStatus(value = HttpStatus.OK)
    public void saveRelation(@Parameter(description = "A JSON value representing the relation.", required = true)
                                       @RequestBody EntityRelation relation) throws ThingsboardException {
        doSave(relation);
    }

    /**
     * 创建或更新一条关系（V2），返回保存后的 {@link EntityRelation}。
     */
    @ApiOperation(value = "Create Relation (saveRelationV2)",
            notes = "Creates or updates a relation between two entities in the platform. " +
                    "Relations unique key is a combination of from/to entity id and relation type group and relation type. " +
                    SECURITY_CHECKS_ENTITIES_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/v2/relation", method = RequestMethod.POST)
    @ResponseStatus(value = HttpStatus.OK)
    public EntityRelation saveRelationV2(@Parameter(description = "A JSON value representing the relation.", required = true)
                                         @RequestBody EntityRelation relation) throws ThingsboardException {
        return doSave(relation);
    }

    private EntityRelation doSave(EntityRelation relation) throws ThingsboardException {
        checkNotNull(relation);
        checkCanCreateRelation(relation.getFrom());
        checkCanCreateRelation(relation.getTo());
        if (relation.getTypeGroup() == null) {
            relation.setTypeGroup(RelationTypeGroup.COMMON);
        }

        return tbEntityRelationService.save(getTenantId(), getCurrentUser().getCustomerId(), relation, getCurrentUser());
    }

    /**
     * 按 from/to/类型删除一条关系。无返回体。
     */
    @ApiOperation(value = "Delete Relation (deleteRelation)",
            notes = "Deletes a relation between two entities in the platform. " + SECURITY_CHECKS_ENTITIES_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relation", method = RequestMethod.DELETE, params = {FROM_ID, FROM_TYPE, RELATION_TYPE, TO_ID, TO_TYPE})
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteRelation(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                               @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                               @Parameter(description = RELATION_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(RELATION_TYPE) String strRelationType,
                               @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION) @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup,
                               @Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                               @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType) throws ThingsboardException {
        doDelete(strFromId, strFromType, strRelationType, strRelationTypeGroup, strToId, strToType);
    }

    /**
     * 按 from/to/类型删除一条关系（V2），返回被删的 {@link EntityRelation}。
     */
    @ApiOperation(value = "Delete Relation (deleteRelationV2)",
            notes = "Deletes a relation between two entities in the platform. " + SECURITY_CHECKS_ENTITIES_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/v2/relation", method = RequestMethod.DELETE, params = {FROM_ID, FROM_TYPE, RELATION_TYPE, TO_ID, TO_TYPE})
    @ResponseStatus(value = HttpStatus.OK)
    public EntityRelation deleteRelationV2(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                                         @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                                         @Parameter(description = RELATION_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(RELATION_TYPE) String strRelationType,
                                         @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION) @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup,
                                         @Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                                         @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType) throws ThingsboardException {
        return doDelete(strFromId, strFromType, strRelationType, strRelationTypeGroup, strToId, strToType);
    }

    private EntityRelation doDelete(String strFromId, String strFromType, String strRelationType, String strRelationTypeGroup, String strToId, String strToType) throws ThingsboardException {
        checkParameter(FROM_ID, strFromId);
        checkParameter(FROM_TYPE, strFromType);
        checkParameter(RELATION_TYPE, strRelationType);
        checkParameter(TO_ID, strToId);
        checkParameter(TO_TYPE, strToType);
        EntityId fromId = EntityIdFactory.getByTypeAndId(strFromType, strFromId);
        EntityId toId = EntityIdFactory.getByTypeAndId(strToType, strToId);
        checkCanCreateRelation(fromId);
        checkCanCreateRelation(toId);

        RelationTypeGroup relationTypeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        EntityRelation relation = new EntityRelation(fromId, toId, strRelationType, relationTypeGroup);
        return tbEntityRelationService.delete(getTenantId(), getCurrentUser().getCustomerId(), relation, getCurrentUser());
    }

    /**
     * 删除指定实体在 COMMON 类型组下的全部 from/to 关系。
     */
    @ApiOperation(value = "Delete common relations (deleteCommonRelations)",
            notes = "Deletes all the relations ('from' and 'to' direction) for the specified entity and relation type group: 'COMMON'. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN','TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.DELETE, params = {"entityId", "entityType"})
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteRelations(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam("entityId") String strId,
                                @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam("entityType") String strType) throws ThingsboardException {
        checkParameter("entityId", strId);
        checkParameter("entityType", strType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strType, strId);
        checkEntityId(entityId, Operation.WRITE);
        tbEntityRelationService.deleteCommonRelations(getTenantId(), getCurrentUser().getCustomerId(), entityId, getCurrentUser());
    }

    /**
     * 按 from/to/类型读取单条关系；不存在则抛异常。
     */
    @ApiOperation(value = "Get Relation (getRelation)",
            notes = "Returns relation object between two specified entities if present. Otherwise throws exception. " + SECURITY_CHECKS_ENTITIES_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relation", method = RequestMethod.GET, params = {FROM_ID, FROM_TYPE, RELATION_TYPE, TO_ID, TO_TYPE})
    @ResponseBody
    public EntityRelation getRelation(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                                      @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                                      @Parameter(description = RELATION_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(RELATION_TYPE) String strRelationType,
                                      @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION) @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup,
                                      @Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                                      @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType) throws ThingsboardException {
        checkParameter(FROM_ID, strFromId);
        checkParameter(FROM_TYPE, strFromType);
        checkParameter(RELATION_TYPE, strRelationType);
        checkParameter(TO_ID, strToId);
        checkParameter(TO_TYPE, strToType);
        EntityId fromId = EntityIdFactory.getByTypeAndId(strFromType, strFromId);
        EntityId toId = EntityIdFactory.getByTypeAndId(strToType, strToId);
        checkEntityId(fromId, Operation.READ);
        checkEntityId(toId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(relationService.getRelation(getTenantId(), fromId, toId, strRelationType, typeGroup));
    }

    /**
     * 列出以指定实体为 from 端的关系（不限关系类型）。
     */
    @ApiOperation(value = "Get List of Relations (findByFrom)",
            notes = "Returns list of relation objects for the specified entity by the 'from' direction. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.GET, params = {FROM_ID, FROM_TYPE})
    @ResponseBody
    public List<EntityRelation> findByFrom(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                                           @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                                           @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                           @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException {
        checkParameter(FROM_ID, strFromId);
        checkParameter(FROM_TYPE, strFromType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strFromType, strFromId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findByFrom(getTenantId(), entityId, typeGroup)));
    }

    /**
     * 列出以指定实体为 from 端的关系信息（含对端名称等）。
     */
    @ApiOperation(value = "Get List of Relation Infos (findInfoByFrom)",
            notes = "Returns list of relation info objects for the specified entity by the 'from' direction. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION + " " + RELATION_INFO_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations/info", method = RequestMethod.GET, params = {FROM_ID, FROM_TYPE})
    @ResponseBody
    public List<EntityRelationInfo> findInfoByFrom(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                                                   @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                                                   @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                                   @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException, ExecutionException, InterruptedException {
        checkParameter(FROM_ID, strFromId);
        checkParameter(FROM_TYPE, strFromType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strFromType, strFromId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findInfoByFrom(getTenantId(), entityId, typeGroup).get()));
    }

    /**
     * 列出以指定实体为 from 端、且关系类型匹配的关系。
     */
    @ApiOperation(value = "Get List of Relations (findByFrom)",
            notes = "Returns list of relation objects for the specified entity by the 'from' direction and relation type. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.GET, params = {FROM_ID, FROM_TYPE, RELATION_TYPE})
    @ResponseBody
    public List<EntityRelation> findByFrom(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_ID) String strFromId,
                                           @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(FROM_TYPE) String strFromType,
                                           @Parameter(description = RELATION_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(RELATION_TYPE) String strRelationType,
                                           @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                           @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException {
        checkParameter(FROM_ID, strFromId);
        checkParameter(FROM_TYPE, strFromType);
        checkParameter(RELATION_TYPE, strRelationType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strFromType, strFromId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findByFromAndType(getTenantId(), entityId, strRelationType, typeGroup)));
    }

    /**
     * 列出以指定实体为 to 端的关系（不限关系类型）。
     */
    @ApiOperation(value = "Get List of Relations (findByTo)",
            notes = "Returns list of relation objects for the specified entity by the 'to' direction. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.GET, params = {TO_ID, TO_TYPE})
    @ResponseBody
    public List<EntityRelation> findByTo(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                                         @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType,
                                         @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                         @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException {
        checkParameter(TO_ID, strToId);
        checkParameter(TO_TYPE, strToType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strToType, strToId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findByTo(getTenantId(), entityId, typeGroup)));
    }

    /**
     * 列出以指定实体为 to 端的关系信息（含对端名称等）。
     */
    @ApiOperation(value = "Get List of Relation Infos (findInfoByTo)",
            notes = "Returns list of relation info objects for the specified entity by the 'to' direction. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION + " " + RELATION_INFO_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations/info", method = RequestMethod.GET, params = {TO_ID, TO_TYPE})
    @ResponseBody
    public List<EntityRelationInfo> findInfoByTo(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                                                 @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType,
                                                 @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                                 @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException, ExecutionException, InterruptedException {
        checkParameter(TO_ID, strToId);
        checkParameter(TO_TYPE, strToType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strToType, strToId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findInfoByTo(getTenantId(), entityId, typeGroup).get()));
    }

    /**
     * 列出以指定实体为 to 端、且关系类型匹配的关系。
     */
    @ApiOperation(value = "Get List of Relations (findByTo)",
            notes = "Returns list of relation objects for the specified entity by the 'to' direction and relation type. " +
                    SECURITY_CHECKS_ENTITY_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.GET, params = {TO_ID, TO_TYPE, RELATION_TYPE})
    @ResponseBody
    public List<EntityRelation> findByTo(@Parameter(description = ENTITY_ID_PARAM_DESCRIPTION, required = true) @RequestParam(TO_ID) String strToId,
                                         @Parameter(description = ENTITY_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(TO_TYPE) String strToType,
                                         @Parameter(description = RELATION_TYPE_PARAM_DESCRIPTION, required = true) @RequestParam(RELATION_TYPE) String strRelationType,
                                         @Parameter(description = RELATION_TYPE_GROUP_PARAM_DESCRIPTION)
                                         @RequestParam(value = "relationTypeGroup", required = false) String strRelationTypeGroup) throws ThingsboardException {
        checkParameter(TO_ID, strToId);
        checkParameter(TO_TYPE, strToType);
        checkParameter(RELATION_TYPE, strRelationType);
        EntityId entityId = EntityIdFactory.getByTypeAndId(strToType, strToId);
        checkEntityId(entityId, Operation.READ);
        RelationTypeGroup typeGroup = parseRelationTypeGroup(strRelationTypeGroup, RelationTypeGroup.COMMON);
        return checkNotNull(filterRelationsByReadPermission(relationService.findByToAndType(getTenantId(), entityId, strRelationType, typeGroup)));
    }

    /**
     * 按 {@link EntityRelationsQuery}（实体、关系类型、深度等）查找相关关系，结果再按 READ 过滤。
     */
    @ApiOperation(value = "Find related entities (findByQuery)",
            notes = "Returns all entities that are related to the specific entity. " +
                    "The entity id, relation type, entity types, depth of the search, and other query parameters defined using complex 'EntityRelationsQuery' object. " +
                    "See 'Model' tab of the Parameters for more info.")
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations", method = RequestMethod.POST)
    @ResponseBody
    public List<EntityRelation> findByQuery(@Parameter(description = "A JSON value representing the entity relations query object.", required = true)
                                            @RequestBody EntityRelationsQuery query) throws ThingsboardException, ExecutionException, InterruptedException {
        checkNotNull(query);
        checkNotNull(query.getParameters());
        checkNotNull(query.getFilters());
        checkEntityId(query.getParameters().getEntityId(), Operation.READ);
        return checkNotNull(filterRelationsByReadPermission(relationService.findByQuery(getTenantId(), query).get()));
    }

    /**
     * 按 {@link EntityRelationsQuery} 查找相关关系信息（含对端名称等）。
     */
    @ApiOperation(value = "Find related entity infos (findInfoByQuery)",
            notes = "Returns all entity infos that are related to the specific entity. " +
                    "The entity id, relation type, entity types, depth of the search, and other query parameters defined using complex 'EntityRelationsQuery' object. " +
                    "See 'Model' tab of the Parameters for more info. " + RELATION_INFO_DESCRIPTION)
    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/relations/info", method = RequestMethod.POST)
    @ResponseBody
    public List<EntityRelationInfo> findInfoByQuery(@Parameter(description = "A JSON value representing the entity relations query object.", required = true)
                                                    @RequestBody EntityRelationsQuery query) throws ThingsboardException, ExecutionException, InterruptedException {
        checkNotNull(query);
        checkNotNull(query.getParameters());
        checkNotNull(query.getFilters());
        checkEntityId(query.getParameters().getEntityId(), Operation.READ);
        return checkNotNull(filterRelationsByReadPermission(relationService.findInfoByQuery(getTenantId(), query).get()));
    }

    private void checkCanCreateRelation(EntityId entityId) throws ThingsboardException {
        SecurityUser currentUser = getCurrentUser();
        var isTenantAdminAndRelateToSelf = currentUser.isTenantAdmin() && currentUser.getTenantId().equals(entityId);
        if (!isTenantAdminAndRelateToSelf) {
            checkEntityId(entityId, Operation.WRITE);
        }
    }

    private <T extends EntityRelation> List<T> filterRelationsByReadPermission(List<T> relationsByQuery) {
        return relationsByQuery.stream().filter(relationByQuery -> {
            try {
                checkEntityId(relationByQuery.getTo(), Operation.READ);
            } catch (ThingsboardException e) {
                return false;
            }
            try {
                checkEntityId(relationByQuery.getFrom(), Operation.READ);
            } catch (ThingsboardException e) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());
    }

    private RelationTypeGroup parseRelationTypeGroup(String strRelationTypeGroup, RelationTypeGroup defaultValue) {
        RelationTypeGroup result = defaultValue;
        if (strRelationTypeGroup != null && strRelationTypeGroup.trim().length() > 0) {
            try {
                result = RelationTypeGroup.valueOf(strRelationTypeGroup);
            } catch (IllegalArgumentException e) {
            }
        }
        return result;
    }

}
