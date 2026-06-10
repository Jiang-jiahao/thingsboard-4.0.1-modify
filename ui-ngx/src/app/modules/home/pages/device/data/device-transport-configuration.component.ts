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

import { ChangeDetectorRef, Component, DestroyRef, forwardRef, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import { Store } from '@ngrx/store';
import { AppState } from '@app/core/core.state';
import { coerceBooleanProperty } from '@angular/cdk/coercion';
import {
  BasicTransportType,
  DeviceTransportConfiguration,
  DeviceTransportType,
  HttpPullRoutingMode,
  normalizeHttpDeviceTransportConfigurationForSave,
  TcpTransportConnectMode,
  TcpWireAuthenticationMode,
  toUiTransportType,
  TransportType,
  UdpWireAuthenticationMode
} from '@shared/models/device.models';
import { deepClone } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'tb-device-transport-configuration',
  templateUrl: './device-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DeviceTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => DeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class DeviceTransportConfigurationComponent implements ControlValueAccessor, OnInit, OnChanges, Validator {

  deviceTransportType = DeviceTransportType;
  basicTransportType = BasicTransportType;

  deviceTransportConfigurationFormGroup: UntypedFormGroup;

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

  /** 当前设备档案的 TCP 链路上鉴权模式（仅 TCP 设备传输子组件使用） */
  @Input()
  tcpWireAuthenticationMode: TcpWireAuthenticationMode | null = null;

  @Input()
  tcpProfileTransportConnectMode: TcpTransportConnectMode | null = null;

  @Input()
  udpWireAuthenticationMode: UdpWireAuthenticationMode | null = null;

  @Input()
  httpPullRoutingMode: HttpPullRoutingMode | null = null;

  @Input()
  httpPushRoutingMode: HttpPullRoutingMode | null = null;

  @Input()
  httpPullProfilePollUrl: string | null = null;

  @Input()
  deviceProfileId: string | null = null;

  @Input()
  editingDeviceId: string | null = null;

  /** 设备档案传输类型（优先；HTTP 档案为 UI 类型 HTTP） */
  @Input()
  profileTransportType: TransportType;

  /** 来自已保存的 transportConfiguration.type，档案信息未就绪时兜底 */
  private configurationTransportType: TransportType;

  private propagateChange = (v: any) => { };
  private suppressModelUpdate = false;

  get activeTransportType(): TransportType | null {
    return toUiTransportType((this.profileTransportType ?? this.configurationTransportType) as DeviceTransportType);
  }

  constructor(private store: Store<AppState>,
              private fb: UntypedFormBuilder,
              private destroyRef: DestroyRef,
              private cd: ChangeDetectorRef) {
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.profileTransportType || changes['profileTransportType']) {
      this.cd.markForCheck();
    }
  }

  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }

  registerOnTouched(fn: any): void {
  }

  ngOnInit() {
    this.deviceTransportConfigurationFormGroup = this.fb.group({
      configuration: [null, Validators.required]
    });
    this.deviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.deviceTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.deviceTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }

  writeValue(value: DeviceTransportConfiguration | null): void {
    if (value?.type) {
      this.configurationTransportType = value.type;
    }
    const configuration = deepClone(value);
    if (configuration) {
      delete configuration.type;
    }
    this.suppressModelUpdate = true;
    this.deviceTransportConfigurationFormGroup.patchValue({configuration}, {emitEvent: false});
    this.suppressModelUpdate = false;
    this.cd.markForCheck();
  }

  validate(): ValidationErrors | null {
    return this.deviceTransportConfigurationFormGroup.valid ? null : {
      deviceTransportConfiguration: false
    };
  }

  private updateModel() {
    if (this.suppressModelUpdate) {
      return;
    }
    let configuration: DeviceTransportConfiguration = null;
    if (this.deviceTransportConfigurationFormGroup.valid) {
      configuration = this.deviceTransportConfigurationFormGroup.getRawValue().configuration;
      const transportType = configuration?.type ?? this.configurationTransportType ?? this.profileTransportType;
      const httpPullProfileActive = this.httpPullProfilePollUrl != null;
      configuration = normalizeHttpDeviceTransportConfigurationForSave(
        transportType,
        configuration,
        httpPullProfileActive
      );
    }
    this.propagateChange(configuration);
  }
}
