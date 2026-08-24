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
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.thingsboard.server.common.data.OtaPackage;
import org.thingsboard.server.common.data.OtaPackageInfo;
import org.thingsboard.server.common.data.SaveOtaPackageInfoRequest;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.OtaPackageId;
import org.thingsboard.server.common.data.ota.ChecksumAlgorithm;
import org.thingsboard.server.common.data.ota.OtaPackageType;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.config.annotations.ApiOperation;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.ota.TbOtaPackageService;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.io.IOException;

import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;
import static org.thingsboard.server.controller.ControllerConstants.DEVICE_PROFILE_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.OTA_PACKAGE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.OTA_PACKAGE_ID_PARAM_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.OTA_PACKAGE_INFO_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.OTA_PACKAGE_TEXT_SEARCH_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_DATA_PARAMETERS;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_NUMBER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.PAGE_SIZE_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_ORDER_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.SORT_PROPERTY_DESCRIPTION;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH;
import static org.thingsboard.server.controller.ControllerConstants.UUID_WIKI_LINK;

/**
 * OTA 固件 / 软件包 REST 入口。
 * <p>
 * <b>职责：</b>管理租户下 OTA 包信息与二进制（创建信息、上传数据、下载、分页、按设备配置文件过滤、删除）。
 * 标题 + 版本在租户内唯一；被设备或设备配置文件引用时不可删除。
 * <p>
 * <b>URL：</b>{@code /api}（{@code /otaPackage*}、{@code /otaPackages*}）
 * <p>
 * <b>权限：</b>写/删/下载仅 {@code TENANT_ADMIN}；info 查询 {@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。
 * 写/删会校验资源 {@code OTA_PACKAGE}。
 * <p>
 * <b>下游：</b>{@link TbOtaPackageService}、{@code otaPackageService}（{@link BaseController}）
 */
@Slf4j
@RestController
@TbCoreComponent
@RequestMapping("/api")
@RequiredArgsConstructor
public class OtaPackageController extends BaseController {

    private final TbOtaPackageService tbOtaPackageService;

    public static final String OTA_PACKAGE_ID = "otaPackageId";
    public static final String CHECKSUM_ALGORITHM = "checksumAlgorithm";

    /**
     * 按 id 下载 OTA 包二进制。URL 型包（无本地数据）返回 400。
     * <p>
     * 权限：{@code TENANT_ADMIN}；实体 READ。
     */
    @ApiOperation(value = "Download OTA Package (downloadOtaPackage)", notes = "Download OTA Package based on the provided OTA Package Id." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority( 'TENANT_ADMIN')")
    @RequestMapping(value = "/otaPackage/{otaPackageId}/download", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<org.springframework.core.io.Resource> downloadOtaPackage(@Parameter(description = OTA_PACKAGE_ID_PARAM_DESCRIPTION)
                                                                                   @PathVariable(OTA_PACKAGE_ID) String strOtaPackageId) throws ThingsboardException {
        checkParameter(OTA_PACKAGE_ID, strOtaPackageId);
        OtaPackageId otaPackageId = new OtaPackageId(toUUID(strOtaPackageId));
        OtaPackage otaPackage = checkOtaPackageId(otaPackageId, Operation.READ);

        if (otaPackage.hasUrl()) {
            return ResponseEntity.badRequest().build();
        }

        ByteArrayResource resource = new ByteArrayResource(otaPackage.getData().array());
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + otaPackage.getFileName())
                .header("x-filename", otaPackage.getFileName())
                .contentLength(resource.contentLength())
                .contentType(parseMediaType(otaPackage.getContentType()))
                .body(resource);
    }

    /**
     * 按 id 查询 OTA 包信息（不含二进制）。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@code otaPackageService}。
     */
    @ApiOperation(value = "Get OTA Package Info (getOtaPackageInfoById)",
            notes = "Fetch the OTA Package Info object based on the provided OTA Package Id. " +
                    OTA_PACKAGE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/otaPackage/info/{otaPackageId}", method = RequestMethod.GET)
    @ResponseBody
    public OtaPackageInfo getOtaPackageInfoById(@Parameter(description = OTA_PACKAGE_ID_PARAM_DESCRIPTION)
                                                @PathVariable(OTA_PACKAGE_ID) String strOtaPackageId) throws ThingsboardException {
        checkParameter(OTA_PACKAGE_ID, strOtaPackageId);
        OtaPackageId otaPackageId = new OtaPackageId(toUUID(strOtaPackageId));
        return checkNotNull(otaPackageService.findOtaPackageInfoById(getTenantId(), otaPackageId));
    }

    /**
     * 按 id 查询完整 OTA 包（含数据）。校验归属当前租户。
     * <p>
     * 权限：{@code TENANT_ADMIN}；实体 READ。
     */
    @ApiOperation(value = "Get OTA Package (getOtaPackageById)",
            notes = "Fetch the OTA Package object based on the provided OTA Package Id. " +
                    "The server checks that the OTA Package is owned by the same tenant. " + OTA_PACKAGE_DESCRIPTION + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/otaPackage/{otaPackageId}", method = RequestMethod.GET)
    @ResponseBody
    public OtaPackage getOtaPackageById(@Parameter(description = OTA_PACKAGE_ID_PARAM_DESCRIPTION)
                                        @PathVariable(OTA_PACKAGE_ID) String strOtaPackageId) throws ThingsboardException {
        checkParameter(OTA_PACKAGE_ID, strOtaPackageId);
        OtaPackageId otaPackageId = new OtaPackageId(toUUID(strOtaPackageId));
        return checkOtaPackageId(otaPackageId, Operation.READ);
    }

    /**
     * 创建或更新 OTA 包信息。新建时平台生成 id；标题 + 版本在租户内唯一。
     * <p>
     * 权限：{@code TENANT_ADMIN}；资源 {@code OTA_PACKAGE}。下游 {@link TbOtaPackageService#save}。
     */
    @ApiOperation(value = "Create Or Update OTA Package Info (saveOtaPackageInfo)",
            notes = "Create or update the OTA Package Info. When creating OTA Package Info, platform generates OTA Package id as " + UUID_WIKI_LINK +
                    "The newly created OTA Package id will be present in the response. " +
                    "Specify existing OTA Package id to update the OTA Package Info. " +
                    "Referencing non-existing OTA Package Id will cause 'Not Found' error. " +
                    "\n\nOTA Package combination of the title with the version is unique in the scope of tenant. " + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/otaPackage", method = RequestMethod.POST)
    @ResponseBody
    public OtaPackageInfo saveOtaPackageInfo(@Parameter(description = "A JSON value representing the OTA Package.")
                                             @RequestBody SaveOtaPackageInfoRequest otaPackageInfo) throws ThingsboardException {
        otaPackageInfo.setTenantId(getTenantId());
        checkEntity(otaPackageInfo.getId(), otaPackageInfo, Resource.OTA_PACKAGE);

        return tbOtaPackageService.save(otaPackageInfo, getCurrentUser());
    }

    /**
     * 为已有 OTA 包信息上传二进制数据（multipart），并按指定算法计算/校验 checksum。
     * <p>
     * 权限：{@code TENANT_ADMIN}。下游 {@link TbOtaPackageService#saveOtaPackageData}。
     */
    @ApiOperation(value = "Save OTA Package data (saveOtaPackageData)",
            notes = "Update the OTA Package. Adds the date to the existing OTA Package Info" + TENANT_AUTHORITY_PARAGRAPH,
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(content = @Content(mediaType = MULTIPART_FORM_DATA_VALUE)))
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/otaPackage/{otaPackageId}", method = RequestMethod.POST, consumes = MULTIPART_FORM_DATA_VALUE)
    @ResponseBody
    public OtaPackageInfo saveOtaPackageData(@Parameter(description = OTA_PACKAGE_ID_PARAM_DESCRIPTION)
                                             @PathVariable(OTA_PACKAGE_ID) String strOtaPackageId,
                                             @Parameter(description = "OTA Package checksum. For example, '0xd87f7e0c'")
                                             @RequestParam(required = false) String checksum,
                                             @Parameter(description = "OTA Package checksum algorithm.", schema = @Schema(allowableValues = {"MD5", "SHA256", "SHA384", "SHA512", "CRC32", "MURMUR3_32", "MURMUR3_128"}))
                                             @RequestParam(CHECKSUM_ALGORITHM) String checksumAlgorithmStr,
                                             @Parameter(description = "OTA Package data.")
                                             @RequestPart MultipartFile file) throws ThingsboardException, IOException {
        checkParameter(OTA_PACKAGE_ID, strOtaPackageId);
        checkParameter(CHECKSUM_ALGORITHM, checksumAlgorithmStr);
        OtaPackageId otaPackageId = new OtaPackageId(toUUID(strOtaPackageId));
        OtaPackageInfo otaPackageInfo = checkOtaPackageInfoId(otaPackageId, Operation.READ);
        ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.valueOf(checksumAlgorithmStr.toUpperCase());
        byte[] data = file.getBytes();
        return tbOtaPackageService.saveOtaPackageData(otaPackageInfo, checksum, checksumAlgorithm,
                data, file.getOriginalFilename(), file.getContentType(), getCurrentUser());
    }

    /**
     * 分页查询当前租户全部 OTA 包信息。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@code otaPackageService}。
     */
    @ApiOperation(value = "Get OTA Package Infos (getOtaPackages)",
            notes = "Returns a page of OTA Package Info objects owned by tenant. " +
                    PAGE_DATA_PARAMETERS + OTA_PACKAGE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/otaPackages", method = RequestMethod.GET)
    @ResponseBody
    public PageData<OtaPackageInfo> getOtaPackages(@Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                   @RequestParam int pageSize,
                                                   @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                   @RequestParam int page,
                                                   @Parameter(description = OTA_PACKAGE_TEXT_SEARCH_DESCRIPTION)
                                                   @RequestParam(required = false) String textSearch,
                                                   @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "type", "title", "version", "tag", "url", "fileName", "dataSize", "checksum"}))
                                                   @RequestParam(required = false) String sortProperty,
                                                   @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
                                                   @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return checkNotNull(otaPackageService.findTenantOtaPackagesByTenantId(getTenantId(), pageLink));
    }

    /**
     * 按设备配置文件与类型（固件/软件）分页查询已有数据的 OTA 包信息。
     * <p>
     * 权限：{@code TENANT_ADMIN} 或 {@code CUSTOMER_USER}。下游 {@code otaPackageService}。
     */
    @ApiOperation(value = "Get OTA Package Infos (getOtaPackages)",
            notes = "Returns a page of OTA Package Info objects owned by tenant. " +
                    PAGE_DATA_PARAMETERS + OTA_PACKAGE_INFO_DESCRIPTION + TENANT_OR_CUSTOMER_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN', 'CUSTOMER_USER')")
    @RequestMapping(value = "/otaPackages/{deviceProfileId}/{type}", method = RequestMethod.GET)
    @ResponseBody
    public PageData<OtaPackageInfo> getOtaPackages(@Parameter(description = DEVICE_PROFILE_ID_PARAM_DESCRIPTION)
                                                   @PathVariable("deviceProfileId") String strDeviceProfileId,
                                                   @Parameter(description = "OTA Package type.", schema = @Schema(allowableValues = {"FIRMWARE", "SOFTWARE"}))
                                                   @PathVariable("type") String strType,
                                                   @Parameter(description = PAGE_SIZE_DESCRIPTION, required = true)
                                                   @RequestParam int pageSize,
                                                   @Parameter(description = PAGE_NUMBER_DESCRIPTION, required = true)
                                                   @RequestParam int page,
                                                   @Parameter(description = OTA_PACKAGE_TEXT_SEARCH_DESCRIPTION)
                                                   @RequestParam(required = false) String textSearch,
                                                   @Parameter(description = SORT_PROPERTY_DESCRIPTION, schema = @Schema(allowableValues = {"createdTime", "type", "title", "version", "tag", "url", "fileName", "dataSize", "checksum"}))
                                                   @RequestParam(required = false) String sortProperty,
                                                   @Parameter(description = SORT_ORDER_DESCRIPTION, schema = @Schema(allowableValues = {"ASC", "DESC"}))
                                                   @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        checkParameter("deviceProfileId", strDeviceProfileId);
        checkParameter("type", strType);
        PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
        return checkNotNull(otaPackageService.findTenantOtaPackagesByTenantIdAndDeviceProfileIdAndTypeAndHasData(getTenantId(),
                new DeviceProfileId(toUUID(strDeviceProfileId)), OtaPackageType.valueOf(strType), pageLink));
    }

    /**
     * 删除 OTA 包。id 不存在或仍被设备/设备配置文件引用会报错。
     * <p>
     * 权限：{@code TENANT_ADMIN}；实体 DELETE。下游 {@link TbOtaPackageService#delete}。
     */
    @ApiOperation(value = "Delete OTA Package (deleteOtaPackage)",
            notes = "Deletes the OTA Package. Referencing non-existing OTA Package Id will cause an error. " +
                    "Can't delete the OTA Package if it is referenced by existing devices or device profile." + TENANT_AUTHORITY_PARAGRAPH)
    @PreAuthorize("hasAnyAuthority('TENANT_ADMIN')")
    @RequestMapping(value = "/otaPackage/{otaPackageId}", method = RequestMethod.DELETE)
    @ResponseBody
    public void deleteOtaPackage(@Parameter(description = OTA_PACKAGE_ID_PARAM_DESCRIPTION)
                                 @PathVariable("otaPackageId") String strOtaPackageId) throws ThingsboardException {
        checkParameter(OTA_PACKAGE_ID, strOtaPackageId);
        OtaPackageId otaPackageId = new OtaPackageId(toUUID(strOtaPackageId));
        OtaPackageInfo otaPackageInfo = checkOtaPackageInfoId(otaPackageId, Operation.DELETE);
        tbOtaPackageService.delete(otaPackageInfo, getCurrentUser());
    }

}
