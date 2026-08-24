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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceProfileInfo;
import org.thingsboard.server.common.data.EntityInfo;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.dao.resource.ImageService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.device.profile.TbDeviceProfileService;
import org.thingsboard.server.service.security.model.SecurityUser;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.List;
import java.util.UUID;

import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_DATA;
import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_ID;
import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_INFO_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_TEXT_SEARCH_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.INLINE_IMAGES;
import static org.thingsboard.server.controller.ControllerConstants.INLINE_IMAGES_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.NEW_LINE;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.UUID_WIKI_LINK;

/**
 * 设备配置文件（Device Profile）REST 入口。
 * <p>
 * 配置文件定义传输协议、默认规则链、告警规则、配置描述等，同一租户下名称唯一，
 * 且只能有一个 default。仅在 {@link TbCoreComponent} 中生效。
 * <p>
 * <b>URL 前缀：</b>{@code /api}。完整资源路径如 {@code /deviceProfile}、
 * {@code /deviceProfiles}、{@code /deviceProfileInfo}、{@code /deviceProfile/names}。
 * <p>
 * <b>权限：</b>写操作（保存/删除/设默认）仅 TENANT_ADMIN；读 Info / 名称列表
 * 对 TENANT_ADMIN、CUSTOMER_USER 开放。
 * <p>
 * <b>下游：</b>{@link TbDeviceProfileService} 负责写路径；查询走基类
 * {@code deviceProfileService}。可选把图片内联进响应（{@link ImageService}），
 * 以及按配置文件收集遥测/属性 key（{@link TimeseriesService} / {@code attributesService}）。
 *
 * @see TbDeviceProfileService
 * @see ImageService
 * @see TimeseriesService
 */
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class DeviceProfileController extends BaseController {

    private final TbDeviceProfileService tbDeviceProfileService;
    private final ImageService imageService;

    @Autowired
    private TimeseriesService timeseriesService;

    /**
     * 按 Id 读取完整设备配置文件。
     * <p>
     * 校验当前租户拥有该配置文件。{@code inlineImages=true} 时把描述里的图片资源内联进 JSON。
     */
    @ApiOperation(value = "Get Device Profile (getDeviceProfileById)",
            notes = "Fetch the Device Profile object based on the provided Device Profile Id. " +
                    "The server checks that the device profile is owned by the same tenant. " + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile/{deviceProfileId}", method = RequestMethod.GET)
    @ResponseBody
    public DeviceProfile getDeviceProfileById(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @PathVariable(DEVICE_PROFILE_ID) String strDeviceProfileId,
            @Parameter(description = INLINE_IMAGES_DESCRIPTION)
            @RequestParam(value = INLINE_IMAGES, required = false) boolean inlineImages) throws ThingsboardException {
        checkParameter(DEVICE_PROFILE_ID, strDeviceProfileId);
        DeviceProfileId deviceProfileId = new DeviceProfileId(toUUID(strDeviceProfileId));
        var result = checkDeviceProfileId(deviceProfileId, Operation.READ);
        if (inlineImages) {
            result = imageService.inlineImage(result);
        }
        return result;
    }

    /**
     * 按 Id 读取设备配置文件摘要（{@link DeviceProfileInfo}），不含完整配置体。
     */
    @ApiOperation(value = "Get Device Profile Info (getDeviceProfileInfoById)",
            notes = "Fetch the Device Profile Info object based on the provided Device Profile Id. "
                    + DEVICE_PROFILE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/deviceProfileInfo/{deviceProfileId}", method = RequestMethod.GET)
    @ResponseBody
    public DeviceProfileInfo getDeviceProfileInfoById(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @PathVariable(DEVICE_PROFILE_ID) String strDeviceProfileId) throws ThingsboardException {
        checkParameter(DEVICE_PROFILE_ID, strDeviceProfileId);
        DeviceProfileId deviceProfileId = new DeviceProfileId(toUUID(strDeviceProfileId));
        return new DeviceProfileInfo(checkDeviceProfileId(deviceProfileId, Operation.READ));
    }

    /**
     * 读取当前租户的默认设备配置文件摘要。
     */
    @ApiOperation(value = "Get Default Device Profile (getDefaultDeviceProfileInfo)",
            notes = "Fetch the Default Device Profile Info object. " +
                    DEVICE_PROFILE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/deviceProfileInfo/default", method = RequestMethod.GET)
    @ResponseBody
    public DeviceProfileInfo getDefaultDeviceProfileInfo() throws ThingsboardException {
        return checkNotNull(deviceProfileService.findDefaultDeviceProfileInfo(getTenantId()));
    }

    /**
     * 收集指定配置文件下设备用过的遥测 key，供 UI 自动补全。
     * <p>
     * 未传 {@code deviceProfileId} 时在全租户范围内去重。实现会限制参与扫描的设备数量。
     */
    @ApiOperation(value = "Get time series keys (getTimeseriesKeys)",
            notes = "Get a set of unique time series keys used by devices that belong to specified profile. " +
                    "If profile is not set returns a list of unique keys among all profiles. " +
                    "The call is used for auto-complete in the UI forms. " +
                    "The implementation limits the number of devices that participate in search to 100 as a trade of between accurate results and time-consuming queries. " +
                    TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile/devices/keys/timeseries", method = RequestMethod.GET)
    @ResponseBody
    public List<String> getTimeseriesKeys(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @RequestParam(name = DEVICE_PROFILE_ID, required = false) String deviceProfileIdStr) throws ThingsboardException {
        DeviceProfileId deviceProfileId;
        if (StringUtils.isNotEmpty(deviceProfileIdStr)) {
            deviceProfileId = new DeviceProfileId(UUID.fromString(deviceProfileIdStr));
            checkDeviceProfileId(deviceProfileId, Operation.READ);
        } else {
            deviceProfileId = null;
        }

        return timeseriesService.findAllKeysByDeviceProfileId(getTenantId(), deviceProfileId);
    }

    /**
     * 收集指定配置文件下设备用过的属性 key，供 UI 自动补全。
     * <p>
     * 未传 {@code deviceProfileId} 时在全租户范围内去重。
     */
    @ApiOperation(value = "Get attribute keys (getAttributesKeys)",
            notes = "Get a set of unique attribute keys used by devices that belong to specified profile. " +
                    "If profile is not set returns a list of unique keys among all profiles. " +
                    "The call is used for auto-complete in the UI forms. " +
                    "The implementation limits the number of devices that participate in search to 100 as a trade of between accurate results and time-consuming queries. " +
                    TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile/devices/keys/attributes", method = RequestMethod.GET)
    @ResponseBody
    public List<String> getAttributesKeys(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @RequestParam(name = DEVICE_PROFILE_ID, required = false) String deviceProfileIdStr) throws ThingsboardException {
        DeviceProfileId deviceProfileId;
        if (StringUtils.isNotEmpty(deviceProfileIdStr)) {
            deviceProfileId = new DeviceProfileId(UUID.fromString(deviceProfileIdStr));
            checkDeviceProfileId(deviceProfileId, Operation.READ);
        } else {
            deviceProfileId = null;
        }

        return attributesService.findAllKeysByDeviceProfileId(getTenantId(), deviceProfileId);
    }

    /**
     * 创建或更新设备配置文件。
     * <p>
     * 无 {@code id} 为新建（平台生成 UUID）；指定已有 id 则更新。名称在租户内唯一，
     * 每个租户只能有一个 default。
     */
    @ApiOperation(value = "Create Or Update Device Profile (saveDeviceProfile)",
            notes = "Create or update the Device Profile. When creating device profile, platform generates device profile id as " + UUID_WIKI_LINK +
                    "The newly created device profile id will be present in the response. " +
                    "Specify existing device profile id to update the device profile. " +
                    "Referencing non-existing device profile Id will cause 'Not Found' error. " + NEW_LINE +
                    "Device profile name is unique in the scope of tenant. Only one 'default' device profile may exist in scope of tenant." + DEVICE_PROFILE_DATA +
                    "Remove 'id', 'tenantId' from the request body example (below) to create new Device Profile entity. " +
                    TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile", method = RequestMethod.POST)
    @ResponseBody
    public DeviceProfile saveDeviceProfile(
            @Parameter(description = "A JSON value representing the device profile.")
            @RequestBody DeviceProfile deviceProfile) throws Exception {
        deviceProfile.setTenantId(getTenantId());
        checkEntity(deviceProfile.getId(), deviceProfile, Resource.DEVICE_PROFILE);
        return tbDeviceProfileService.save(deviceProfile, getCurrentUser());
    }

    /**
     * 删除设备配置文件。
     * <p>
     * 仍被设备引用时不能删。
     */
    @ApiOperation(value = "Delete device profile (deleteDeviceProfile)",
            notes = "Deletes the device profile. Referencing non-existing device profile Id will cause an error. " +
                    "Can't delete the device profile if it is referenced by existing devices." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile/{deviceProfileId}", method = RequestMethod.DELETE)
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteDeviceProfile(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @PathVariable(DEVICE_PROFILE_ID) String strDeviceProfileId) throws ThingsboardException {
        checkParameter(DEVICE_PROFILE_ID, strDeviceProfileId);
        DeviceProfileId deviceProfileId = new DeviceProfileId(toUUID(strDeviceProfileId));
        DeviceProfile deviceProfile = checkDeviceProfileId(deviceProfileId, Operation.DELETE);
        tbDeviceProfileService.delete(deviceProfile, getCurrentUser());
    }

    /**
     * 把指定配置文件设为租户默认，同时取消原先的 default 标记。
     */
    @ApiOperation(value = "Make Device Profile Default (setDefaultDeviceProfile)",
            notes = "Marks device profile as default within a tenant scope." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfile/{deviceProfileId}/default", method = RequestMethod.POST)
    @ResponseBody
    public DeviceProfile setDefaultDeviceProfile(
            @Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
            @PathVariable(DEVICE_PROFILE_ID) String strDeviceProfileId) throws ThingsboardException {
        checkParameter(DEVICE_PROFILE_ID, strDeviceProfileId);
        DeviceProfileId deviceProfileId = new DeviceProfileId(toUUID(strDeviceProfileId));
        DeviceProfile deviceProfile = checkDeviceProfileId(deviceProfileId, Operation.WRITE);
        DeviceProfile previousDefaultDeviceProfile = deviceProfileService.findDefaultDeviceProfile(getTenantId());
        return tbDeviceProfileService.setDefaultDeviceProfile(deviceProfile, previousDefaultDeviceProfile, getCurrentUser());
    }

    /**
     * 分页列出当前租户的完整设备配置文件。
     */
    @ApiOperation(value = "Get Device Profiles (getDeviceProfiles)",
            notes = "Returns a page of devices profile objects owned by tenant. " +
                    PAGE_DATA_PARAMETERS + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/deviceProfiles", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<DeviceProfile> getDeviceProfiles(
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = DEVICE_PROFILE_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type", "transportType", "description", "isDefault"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return checkNotNull(deviceProfileService.findDeviceProfiles(getTenantId(), pageLink));
    }

    /**
     * 分页列出设备配置文件摘要，可按传输类型（DEFAULT / MQTT / COAP / LWM2M / SNMP）过滤。
     */
    @ApiOperation(value = "Get Device Profiles for transport type (getDeviceProfileInfos)",
            notes = "Returns a page of devices profile info objects owned by tenant. " +
                    PAGE_DATA_PARAMETERS + DEVICE_PROFILE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/deviceProfileInfos", params = {"pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<DeviceProfileInfo> getDeviceProfileInfos(
            @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
            @RequestParam int pageSize,
            @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
            @RequestParam int page,
            @Parameter(description = DEVICE_PROFILE_TEXT_SEARCH_DESCRIPTION)
            @RequestParam(required = false) String textSearch,
            @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "name", "type", "transportType", "description", "isDefault"}))
            @RequestParam(required = false) String sortProperty,
            @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
            @RequestParam(required = false) String sortOrder,
            @Parameter(description = "Type of the transport", schema = @Schema(allowableValues = {"DEFAULT", "MQTT", "COAP", "LWM2M", "SNMP"}))
            @RequestParam(required = false) String transportType) throws ThingsboardException {
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return checkNotNull(deviceProfileService.findDeviceProfileInfos(getTenantId(), pageLink, transportType));
    }

    /**
     * 返回当前租户下设备配置文件名称列表。
     * <p>
     * {@code activeOnly=true} 时只返回已被设备引用的配置文件名。
     */
    @ApiOperation(value = "Get Device Profile names (getDeviceProfileNames)",
            notes = "Returns a set of unique device profile names owned by the tenant."
                    + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/deviceProfile/names", method = RequestMethod.GET)
    @ResponseBody
    public List<EntityInfo> getDeviceProfileNames(
            @Parameter(description = "Flag indicating whether to retrieve exclusively the names of device profiles that are referenced by tenant's devices.")
            @RequestParam(value = "activeOnly", required = false, defaultValue = "false") boolean activeOnly) throws ThingsboardException {
        SecurityUser user = getCurrentUser();
        TenantId tenantId = user.getTenantId();
        return checkNotNull(deviceProfileService.findDeviceProfileNamesByTenantId(tenantId, activeOnly));
    }

}
