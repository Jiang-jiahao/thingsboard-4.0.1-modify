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
import { Component, DestroyRef, forwardRef, Input, OnInit } from '@angular/core';
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
  DeviceTransportType,
  UdpDeviceTransportConfiguration,
  UdpWireAuthenticationMode
} from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'tb-udp-device-transport-configuration',
  templateUrl: './udp-device-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => UdpDeviceTransportConfigurationComponent),
      multi: true
    }, {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => UdpDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class UdpDeviceTransportConfigurationComponent implements ControlValueAccessor, OnInit, Validator {

  tcpDeviceTransportConfigurationFormGroup: UntypedFormGroup;

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

  private udpWireAuthMode: UdpWireAuthenticationMode | null = null;

  @Input()
  set udpWireAuthenticationMode(mode: UdpWireAuthenticationMode | null) {
    const prev = this.udpWireAuthMode;
    this.udpWireAuthMode = mode;
    if (this.tcpDeviceTransportConfigurationFormGroup
        && prev === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID
        && mode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue(
        { udpWireAuthPayloadDeviceId: '' },
        { emitEvent: true }
      );
    }
    this.applyDeferredPayloadDeviceIdValidators();
  }

  get showUdpWireAuthPayloadDeviceId(): boolean {
    return this.udpWireAuthMode === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** UDP 设备向平台监听端口发报文；NONE 鉴权时可填期望源 IP 以区分同端口多设备 */
  get showUdpSourceHostField(): boolean {
    return this.udpWireAuthMode === UdpWireAuthenticationMode.NONE;
  }

  get showUdpDeviceHint(): boolean {
    return this.udpWireAuthMode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
      && this.udpWireAuthMode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
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
    this.tcpDeviceTransportConfigurationFormGroup = this.fb.group({
      sourceHost: [''],
      udpWireAuthPayloadDeviceId: ['']
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
    this.applyDeferredPayloadDeviceIdValidators();
  }

  private applyDeferredPayloadDeviceIdValidators(): void {
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    if (!grp) {
      return;
    }
    const pidCtrl = grp.get('udpWireAuthPayloadDeviceId');
    if (this.udpWireAuthMode === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      pidCtrl.setValidators([(c) => {
        const v = c.value;
        return v != null && String(v).trim().length ? null : { required: true };
      }]);
    } else {
      pidCtrl.clearValidators();
    }
    pidCtrl.updateValueAndValidity({ emitEvent: true });
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }

  writeValue(value: UdpDeviceTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue({
        sourceHost: value.sourceHost || '',
        udpWireAuthPayloadDeviceId: value.udpWireAuthPayloadDeviceId || ''
      }, {emitEvent: false});
    }
    this.applyDeferredPayloadDeviceIdValidators();
  }

  validate(): ValidationErrors | null {
    return this.tcpDeviceTransportConfigurationFormGroup.valid ? null : {udpDeviceTransport: false};
  }

  private updateModel() {
    if (!this.tcpDeviceTransportConfigurationFormGroup.valid) {
      this.propagateChange(null);
      return;
    }
    const v = this.tcpDeviceTransportConfigurationFormGroup.getRawValue();
    const configuration: UdpDeviceTransportConfiguration = {
      type: DeviceTransportType.UDP
    };
    const sh = v.sourceHost?.trim();
    if (sh) {
      configuration.sourceHost = sh;
    }
    const pid = v.udpWireAuthPayloadDeviceId?.trim();
    if (pid) {
      configuration.udpWireAuthPayloadDeviceId = pid;
    }
    this.propagateChange(configuration);
  }
}
