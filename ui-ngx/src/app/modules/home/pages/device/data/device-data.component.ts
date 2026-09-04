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

import { Component, DestroyRef, forwardRef, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import {
  ControlValueAccessor,
  UntypedFormBuilder,
  UntypedFormGroup,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import { Store } from '@ngrx/store';
import { AppState } from '@app/core/core.state';
import { coerceBooleanProperty } from '@angular/cdk/coercion';
import {
  createDeviceTransportConfiguration,
  DeviceData,
  DeviceProfileInfo,
  DeviceTransportType,
  HttpPullRoutingMode,
  hasDeviceTransportConfiguration,
  TcpTransportConnectMode,
  TcpWireAuthenticationMode,
  toUiTransportType,
  TransportType,
  UdpWireAuthenticationMode,
  deviceProfileTypeConfigurationInfoMap
} from '@shared/models/device.models';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'tb-device-data',
  templateUrl: './device-data.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DeviceDataComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => DeviceDataComponent),
      multi: true
    },
  ]
})
export class DeviceDataComponent implements ControlValueAccessor, OnInit, OnChanges, Validator {

  deviceDataFormGroup: UntypedFormGroup;

  
  @Input()
  deviceProfile: DeviceProfileInfo;

  /** 来自设备页：当前设备档案的 TCP 链路上鉴权模式（用于 TCP 设备传输表单项显隐） */
  @Input()
  tcpWireAuthenticationMode: TcpWireAuthenticationMode | null = null;

  @Input()
  udpWireAuthenticationMode: UdpWireAuthenticationMode | null = null;

  /** 来自设备页：当前设备档案的 TCP CLIENT/SERVER（用于隐藏 SERVER 档案下无意义的 CLIENT 对端表单项） */
  @Input()
  tcpProfileTransportConnectMode: TcpTransportConnectMode | null = null;

  @Input()
  httpPullRoutingMode: HttpPullRoutingMode | null = null;

  @Input()
  httpPushRoutingMode: HttpPullRoutingMode | null = null;

  @Input()
  httpPullProfilePollUrl: string | null = null;

  @Input()
  mqttPullProfileActive = false;

  @Input()
  deviceProfileId: string | null = null;

  @Input()
  editingDeviceId: string | null = null;

  /** 来自设备详情页：tenant | customer | customer_user | edge | edge_customer_user */
  @Input()
  deviceScope: string;

  private lastWrittenValue: DeviceData | null = null;

  private requiredValue: boolean;
  get required(): boolean {
    return this.requiredValue;
  }
  @Input()
  set required(value: boolean) {
    this.requiredValue = coerceBooleanProperty(value);
  }

  @Input()
  disabled: boolean;

  displayDeviceConfiguration: boolean;
  displayTransportConfiguration: boolean;

  /** 当前所选设备配置文件为 TCP 时，展示协议模板库入口（模板负载在配置文件中维护） */
  get showTcpProtocolTemplateHelp(): boolean {
    return this.deviceProfile?.transportType === DeviceTransportType.TCP
      || this.deviceProfile?.transportType === DeviceTransportType.UDP;
  }

  get profileUiTransportType(): TransportType | null {
    const fromProfile = this.deviceProfile?.transportType;
    if (fromProfile) {
      return toUiTransportType(fromProfile);
    }
    const fromConfig = this.deviceDataFormGroup?.get('transportConfiguration')?.value?.type;
    return toUiTransportType(fromConfig);
  }

  private propagateChange = (v: any) => { };

  constructor(private store: Store<AppState>,
              private fb: UntypedFormBuilder,
              private destroyRef: DestroyRef) {
  }

  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }

  registerOnTouched(fn: any): void {
  }

  ngOnInit() {
    this.deviceDataFormGroup = this.fb.group({
      configuration: [null, Validators.required],
      transportConfiguration: [null, Validators.required]
    });
    this.deviceDataFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.deviceDataFormGroup.disable({emitEvent: false});
    } else {
      this.deviceDataFormGroup.enable({emitEvent: false});
    }
  }

  
  ngOnChanges(changes: SimpleChanges): void {
    if ((changes.deviceProfile || changes.mqttPullProfileActive) && this.deviceDataFormGroup) {
      this.applyVisibilityFromValue(this.lastWrittenValue);
      this.ensureTransportConfigurationForProfile();
    }
  }

  /** 添加设备向导等场景：档案已选但 deviceData 尚未写入时，先初始化传输配置 */
  private ensureTransportConfigurationForProfile(): void {
    const transportType = this.deviceProfile?.transportType;
    if (!transportType || !this.deviceDataFormGroup) {
      return;
    }
    if (!this.needsDeviceTransportConfiguration(transportType)) {
      return;
    }
    const ctrl = this.deviceDataFormGroup.get('transportConfiguration');
    const current = ctrl?.value;
    if (!current?.type || current.type !== transportType) {
      ctrl.patchValue(createDeviceTransportConfiguration(transportType), { emitEvent: false });
    }
  }

  writeValue(value: DeviceData | null): void {
    this.lastWrittenValue = value;
    this.applyVisibilityFromValue(value);
    this.deviceDataFormGroup.patchValue({configuration: value?.configuration}, {emitEvent: false});
    this.deviceDataFormGroup.patchValue({transportConfiguration: value?.transportConfiguration}, {emitEvent: false});
  }

  private applyVisibilityFromValue(value: DeviceData | null): void {
    /* 新建设备时 value 可能仍为 null，但已选设备档案；须用档案类型决定是否展示传输配置，否则会隐藏 TCP 面板导致未提交 TcpDeviceTransportConfiguration，后端报 DEFERRED_PAYLOAD_DEVICE_ID requires device transport configuration。 */
    const profileType = value?.configuration?.type ?? this.deviceProfile?.type;
    const profileInfo = profileType && deviceProfileTypeConfigurationInfoMap.get(profileType);
    this.displayDeviceConfiguration = !!(profileInfo?.hasDeviceConfiguration);
    const transportType = value?.transportConfiguration?.type ?? this.deviceProfile?.transportType;
    this.displayTransportConfiguration = this.needsDeviceTransportConfiguration(transportType as DeviceTransportType);
  }

  /** MQTT 服务端无设备级字段，不展示空的传输配置；MQTT 客户端仍展示 Broker 等表单。 */
  private needsDeviceTransportConfiguration(transportType: DeviceTransportType | TransportType | null | undefined): boolean {
    if (!transportType) {
      return false;
    }
    if (this.mqttPullProfileActive || transportType === DeviceTransportType.MQTT_PULL) {
      return true;
    }
    return hasDeviceTransportConfiguration(toUiTransportType(transportType));
  }

  validate(): ValidationErrors | null {
    return this.deviceDataFormGroup.valid ? null : {
      deviceDataForm: false
    };
  }

  private updateModel() {
    let deviceData: DeviceData = null;
    if (this.deviceDataFormGroup.valid) {
      const raw = this.deviceDataFormGroup.getRawValue();
      deviceData = {
        configuration: raw.configuration,
        transportConfiguration: raw.transportConfiguration,
        rpcParamDefaults: this.lastWrittenValue?.rpcParamDefaults,
        rpcParamDefaultsByMethod: this.lastWrittenValue?.rpcParamDefaultsByMethod
      };
    }
    this.propagateChange(deviceData);
  }
}
