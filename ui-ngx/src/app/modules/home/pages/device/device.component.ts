///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { ChangeDetectorRef, Component, DestroyRef, Inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { EntityComponent } from '../../components/entity/entity.component';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import {
  createDeviceConfiguration,
  createDeviceTransportConfiguration, DeviceCredentials,
  DeviceData,
  DeviceInfo,
  DeviceProfile,
  DeviceProfileInfo,
  DeviceProfileType,
  DeviceTransportType,
  extractHttpPullProfileContext,
  extractHttpPushProfileContext,
  extractMqttPullProfileContext,
  HttpPullRoutingMode,
  TcpDeviceProfileTransportConfiguration,
  UdpDeviceProfileTransportConfiguration,
  UdpWireAuthenticationMode,
  TcpTransportConnectMode,
  TcpWireAuthenticationMode,
  GATEWAY_UI_ENABLED
} from '@shared/models/device.models';
import { DeviceProfileService } from '@core/http/device-profile.service';
import { distinctUntilChanged, startWith } from 'rxjs/operators';
import { EntityType } from '@shared/models/entity-type.models';
import { DeviceProfileId } from '@shared/models/id/device-profile-id';
import { NULL_UUID } from '@shared/models/id/has-uuid';
import { ActionNotificationShow } from '@core/notification/notification.actions';
import { TranslateService } from '@ngx-translate/core';
import { EntityTableConfig } from '@home/models/entity/entities-table-config.models';
import { Subject } from 'rxjs';
import { OtaUpdateType } from '@shared/models/ota-package.models';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'tb-device',
  templateUrl: './device.component.html',
  styleUrls: ['./device.component.scss']
})
export class DeviceComponent extends EntityComponent<DeviceInfo> {

  entityType = EntityType;

  deviceCredentials$: Subject<DeviceCredentials>;

  deviceScope: 'tenant' | 'customer' | 'customer_user' | 'edge' | 'edge_customer_user';

  otaUpdateType = OtaUpdateType;

  /** 当前所选设备档案的 TCP 链路上鉴权模式（拉取完整档案后填充） */
  tcpProfileWireAuthMode: TcpWireAuthenticationMode | null = null;
  udpProfileWireAuthMode: UdpWireAuthenticationMode | null = null;

  /** 当前所选设备档案的 TCP 连接模式（CLIENT/SERVER），用于设备传输页表单项显隐 */
  tcpProfileTransportConnectMode: TcpTransportConnectMode | null = null;

  httpPullProfileRoutingMode: HttpPullRoutingMode | null = null;

  httpPushProfileRoutingMode: HttpPullRoutingMode | null = null;

  httpPullProfilePollUrl: string | null = null;

  mqttPullProfileActive = false;

  readonly gatewayUiEnabled = GATEWAY_UI_ENABLED;

  get hidePlatformCredentials(): boolean {
    return this.mqttPullProfileActive
      || this.entity?.deviceData?.transportConfiguration?.type === DeviceTransportType.MQTT_PULL;
  }

  get httpPullDeviceProfileId(): string | null {
    return this.resolveDeviceProfileUuid(this.entityForm?.get('deviceProfileId')?.value);
  }

  get mqttPullDeviceProfileId(): string | null {
    return this.resolveDeviceProfileUuid(this.entityForm?.get('deviceProfileId')?.value);
  }

  constructor(protected store: Store<AppState>,
              protected translate: TranslateService,
              @Inject('entity') protected entityValue: DeviceInfo,
              @Inject('entitiesTableConfig') protected entitiesTableConfigValue: EntityTableConfig<DeviceInfo>,
              public fb: UntypedFormBuilder,
              protected cd: ChangeDetectorRef,
              private destroyRef: DestroyRef,
              private deviceProfileService: DeviceProfileService) {
    super(store, fb, entityValue, entitiesTableConfigValue, cd);
  }

  ngOnInit() {
    this.deviceScope = this.entitiesTableConfig.componentsData.deviceScope;
    this.deviceCredentials$ = this.entitiesTableConfigValue.componentsData.deviceCredentials$;
    super.ngOnInit();
    this.entityForm.get('deviceProfileId').valueChanges.pipe(
      startWith(this.entityForm.get('deviceProfileId').value),
      distinctUntilChanged((a, b) => this.resolveDeviceProfileUuid(a) === this.resolveDeviceProfileUuid(b)),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(profileInfo => this.refreshTcpProfileWireAuth(profileInfo));
  }

  hideDelete() {
    if (this.entitiesTableConfig) {
      return !this.entitiesTableConfig.deleteEnabled(this.entity);
    } else {
      return false;
    }
  }

  isAssignedToCustomer(entity: DeviceInfo): boolean {
    return entity && entity.customerId && entity.customerId.id !== NULL_UUID;
  }

  buildForm(entity: DeviceInfo): UntypedFormGroup {
    const form = this.fb.group(
      {
        name: [entity ? entity.name : '', [Validators.required, Validators.maxLength(255)]],
        deviceProfileId: [entity ? entity.deviceProfileId : null, [Validators.required]],
        firmwareId: [entity ? entity.firmwareId : null],
        softwareId: [entity ? entity.softwareId : null],
        label: [entity ? entity.label : '', [Validators.maxLength(255)]],
        deviceData: [entity ? entity.deviceData : null, [Validators.required]],
        additionalInfo: this.fb.group(
          {
            gateway: [entity && entity.additionalInfo ? entity.additionalInfo.gateway : false],
            overwriteActivityTime: [entity && entity.additionalInfo ? entity.additionalInfo.overwriteActivityTime : false],
            description: [entity && entity.additionalInfo ? entity.additionalInfo.description : ''],
          }
        )
      }
    );
    form.get('deviceProfileId').valueChanges.pipe(
      distinctUntilChanged((prev, curr) => prev?.id === curr?.id),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(profileId => {
      if (profileId && this.isEdit) {
        this.entityForm.patchValue({
          firmwareId: null,
          softwareId: null
        }, {emitEvent: false});
      }
    });
    return form;
  }

  updateForm(entity: DeviceInfo) {
    this.entityForm.patchValue({
      name: entity.name,
      deviceProfileId: entity.deviceProfileId,
      firmwareId: entity.firmwareId,
      softwareId: entity.softwareId,
      label: entity.label,
      deviceData: entity.deviceData,
      additionalInfo: {
        gateway: entity.additionalInfo ? entity.additionalInfo.gateway : false,
        overwriteActivityTime: entity.additionalInfo ? entity.additionalInfo.overwriteActivityTime : false,
        description: entity.additionalInfo ? entity.additionalInfo.description : ''
      }
    });
    // 自动完成返回的 deviceProfile 可能缺少 transportType；与 valueChanges 互补，避免链路上鉴权模式不刷新
    this.refreshTcpProfileWireAuth(this.entityForm.get('deviceProfileId').value);
  }


  onDeviceIdCopied($event) {
    this.store.dispatch(new ActionNotificationShow(
      {
        message: this.translate.instant('device.idCopiedMessage'),
        type: 'success',
        duration: 750,
        verticalPosition: 'bottom',
        horizontalPosition: 'right'
      }));
  }

  onDeviceProfileUpdated() {
    this.entitiesTableConfig.updateData(false);
  }

  onDeviceProfileChanged(deviceProfile: DeviceProfileInfo) {
    if (!deviceProfile || (!this.isEdit && !this.isAdd)) {
      return;
    }
    const prev: DeviceData = this.entityForm.getRawValue().deviceData;
    const apply = (profileType: DeviceProfileType, transportType: DeviceTransportType, fullProfile?: DeviceProfile) => {
      if (!transportType) {
        return;
      }
      if (!prev) {
        const deviceData: DeviceData = {
          configuration: createDeviceConfiguration(profileType),
          transportConfiguration: createDeviceTransportConfiguration(transportType, fullProfile)
        };
        this.entityForm.patchValue({deviceData});
        this.entityForm.markAsDirty();
        return;
      }
      let next: DeviceData = prev;
      if (prev.configuration?.type !== profileType) {
        next = {...next, configuration: createDeviceConfiguration(profileType)};
      }
      if (prev.transportConfiguration?.type !== transportType) {
        next = {
          ...next,
          transportConfiguration: createDeviceTransportConfiguration(transportType, fullProfile)
        };
      }
      if (next !== prev) {
        this.entityForm.patchValue({deviceData: next});
        this.entityForm.markAsDirty();
      }
    };
    const profileType = deviceProfile.type;
    const transportTypeFromAutocomplete = deviceProfile.transportType;
    const uuid = this.resolveDeviceProfileUuid(deviceProfile);
    if (!uuid) {
      this.applyTcpProfileWireAuthFromDeviceProfile(null);
      apply(profileType, transportTypeFromAutocomplete);
      return;
    }
    /*
     * 始终拉取完整档案：自动完成可能仅有 id/name，或仅有 transportType 而无链路上鉴权模式。
     * 须先于 apply() 写入 tcpProfileWireAuthMode，否则 TCP 传输子组件在 DEFERRED_PAYLOAD_DEVICE_ID 下不展示协议设备 ID 输入框。
     */
    this.deviceProfileService.getDeviceProfile(uuid).subscribe({
      next: (dp: DeviceProfile) => {
        this.applyTcpProfileWireAuthFromDeviceProfile(dp);
        const resolvedProfileType = dp?.type ?? profileType;
        const resolvedTransportType = dp?.transportType ?? transportTypeFromAutocomplete;
        apply(resolvedProfileType, resolvedTransportType, dp);
        this.cd.markForCheck();
      },
      error: () => {
        this.applyTcpProfileWireAuthFromDeviceProfile(null);
        apply(profileType, transportTypeFromAutocomplete);
        this.cd.markForCheck();
      }
    });
  }

  /**
   * deviceProfileId 控件可能是自动完成的 DeviceProfileInfo（id 为 EntityId），
   * 也可能是编辑态从实体带入的 DeviceProfileId（id 为 UUID 字符串）；需统一解析。
   */
  private resolveDeviceProfileUuid(profileRef: DeviceProfileInfo | DeviceProfileId | null | undefined): string | null {
    if (profileRef == null || profileRef.id == null) {
      return null;
    }
    const idField = profileRef.id as string | { id?: string };
    if (typeof idField === 'string') {
      return idField.length ? idField : null;
    }
    if (typeof idField === 'object' && typeof idField.id === 'string' && idField.id.length) {
      return idField.id;
    }
    return null;
  }

  private refreshTcpProfileWireAuth(profileInfo: DeviceProfileInfo | DeviceProfileId | null): void {
    const deviceProfileUuid = this.resolveDeviceProfileUuid(profileInfo);
    if (!deviceProfileUuid) {
      this.applyTcpProfileWireAuthFromDeviceProfile(null);
      this.cd.markForCheck();
      return;
    }
    // 勿依赖 profileInfo.transportType：设备页自动完成可能只带 id/name，transportType 为空会导致永远不拉档案
    const transportType = profileInfo && 'transportType' in profileInfo ? profileInfo.transportType : undefined;
    const needsFullProfile = !transportType
      || transportType === DeviceTransportType.TCP
      || transportType === DeviceTransportType.UDP
      || transportType === DeviceTransportType.HTTP_PULL
      || transportType === DeviceTransportType.MQTT_PULL;
    if (!needsFullProfile) {
      this.applyTcpProfileWireAuthFromDeviceProfile(null);
      this.cd.markForCheck();
      return;
    }
    this.deviceProfileService.getDeviceProfile(deviceProfileUuid).subscribe({
      next: (dp: DeviceProfile) => {
        this.applyTcpProfileWireAuthFromDeviceProfile(dp);
        this.cd.markForCheck();
      },
      error: () => {
        this.applyTcpProfileWireAuthFromDeviceProfile(null);
        this.cd.markForCheck();
      }
    });
  }

  private applyTcpProfileWireAuthFromDeviceProfile(dp: DeviceProfile | null): void {
    this.tcpProfileWireAuthMode = null;
    this.tcpProfileTransportConnectMode = null;
    this.udpProfileWireAuthMode = null;
    this.httpPullProfileRoutingMode = null;
    this.httpPushProfileRoutingMode = null;
    this.httpPullProfilePollUrl = null;
    this.mqttPullProfileActive = false;
    if (!dp) {
      return;
    }
    const httpPullCtx = extractHttpPullProfileContext(dp);
    if (httpPullCtx) {
      this.httpPullProfileRoutingMode = httpPullCtx.routingMode;
      this.httpPullProfilePollUrl = httpPullCtx.pollUrl;
    }
    this.mqttPullProfileActive = extractMqttPullProfileContext(dp) != null;
    const httpPushCtx = extractHttpPushProfileContext(dp);
    if (httpPushCtx) {
      this.httpPushProfileRoutingMode = httpPushCtx.routingMode;
    }
    if (dp.transportType === DeviceTransportType.TCP) {
      const raw = dp.profileData?.transportConfiguration as TcpDeviceProfileTransportConfiguration | undefined;
      if (raw && (raw.type === DeviceTransportType.TCP || raw.type == null || raw.type === undefined)) {
        this.tcpProfileWireAuthMode = raw.tcpWireAuthenticationMode ?? null;
        this.tcpProfileTransportConnectMode = raw.tcpTransportConnectMode ?? null;
      }
      return;
    }
    if (dp.transportType === DeviceTransportType.UDP) {
      const raw = dp.profileData?.transportConfiguration as UdpDeviceProfileTransportConfiguration | undefined;
      if (raw && (raw.type === DeviceTransportType.UDP || raw.type == null || raw.type === undefined)) {
        this.udpProfileWireAuthMode = raw.udpWireAuthenticationMode ?? null;
      }
    }
  }
}
