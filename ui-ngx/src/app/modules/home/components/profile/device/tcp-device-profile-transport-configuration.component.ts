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
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
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
import {
  DeviceTransportType,
  TcpDeviceProfileTransportConfiguration,
  TcpJsonWithoutMethodMode,
  TcpTransportConnectMode,
  TcpTransportFramingMode,
  TcpWireAuthenticationMode,
  TransportTcpDataType
} from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
@Component({
  selector: 'tb-tcp-device-profile-transport-configuration',
  templateUrl: './tcp-device-profile-transport-configuration.component.html',
  styleUrls: ['./tcp-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => TcpDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => TcpDeviceProfileTransportConfigurationComponent),
      multi: true
    }]
})
export class TcpDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {
  tcpTransportFramingModes = Object.values(TcpTransportFramingMode);
  transportTcpDataTypes = Object.values(TransportTcpDataType);
  tcpDeviceProfileTransportConfigurationFormGroup: UntypedFormGroup;
  private destroy$ = new Subject<void>();
  @Input()
  disabled: boolean;
  private propagateChange = (v: any) => {
  };
  constructor(private fb: UntypedFormBuilder) {
  }
  ngOnInit(): void {
    this.tcpDeviceProfileTransportConfigurationFormGroup = this.fb.group({
      tcpTransportConnectMode: [TcpTransportConnectMode.SERVER, Validators.required],
      tcpTransportFramingMode: [TcpTransportFramingMode.LINE, Validators.required],
      tcpFixedFrameLength: [null],
      tcpWireAuthenticationMode: [TcpWireAuthenticationMode.TOKEN, Validators.required],
      tcpJsonWithoutMethodMode: [TcpJsonWithoutMethodMode.TELEMETRY_FLAT, Validators.required],
      tcpOpaqueRuleEngineKey: ['tcpOpaquePayload'],
      dataType: [TransportTcpDataType.JSON, Validators.required]
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpTransportFramingMode').valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe((mode: TcpTransportFramingMode) => {
      const fixedCtrl = this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpFixedFrameLength');
      if (mode === TcpTransportFramingMode.FIXED_LENGTH) {
        fixedCtrl.setValidators([Validators.required, Validators.min(1), Validators.pattern('[0-9]*')]);
      } else {
        fixedCtrl.clearValidators();
        fixedCtrl.patchValue(null, {emitEvent: false});
      }
      fixedCtrl.updateValueAndValidity({emitEvent: false});
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe(() => {
      this.updateModel();
    });
  }
  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }
  registerOnTouched(fn: any): void {
  }
  setDisabledState(isDisabled: boolean) {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceProfileTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceProfileTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }
  writeValue(value: TcpDeviceProfileTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      const dataType = value.transportTcpDataTypeConfiguration?.transportTcpDataType || TransportTcpDataType.JSON;
      this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
        tcpTransportConnectMode: value.tcpTransportConnectMode,
        tcpTransportFramingMode: value.tcpTransportFramingMode,
        tcpFixedFrameLength: value.tcpFixedFrameLength,
        tcpWireAuthenticationMode: value.tcpWireAuthenticationMode,
        tcpJsonWithoutMethodMode: value.tcpJsonWithoutMethodMode,
        tcpOpaqueRuleEngineKey: value.tcpOpaqueRuleEngineKey || 'tcpOpaquePayload',
        dataType
      }, {emitEvent: false});
      const mode = value.tcpTransportFramingMode || TcpTransportFramingMode.LINE;
      const fixedCtrl = this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpFixedFrameLength');
      if (mode === TcpTransportFramingMode.FIXED_LENGTH) {
        fixedCtrl.setValidators([Validators.required, Validators.min(1), Validators.pattern('[0-9]*')]);
      } else {
        fixedCtrl.clearValidators();
      }
      fixedCtrl.updateValueAndValidity({emitEvent: false});
    }
  }
  private updateModel() {
    const v = this.tcpDeviceProfileTransportConfigurationFormGroup.getRawValue();
    const configuration: TcpDeviceProfileTransportConfiguration = {
      tcpTransportConnectMode: v.tcpTransportConnectMode,
      tcpTransportFramingMode: v.tcpTransportFramingMode,
      tcpWireAuthenticationMode: v.tcpWireAuthenticationMode,
      tcpJsonWithoutMethodMode: v.tcpJsonWithoutMethodMode,
      tcpOpaqueRuleEngineKey: v.tcpOpaqueRuleEngineKey,
      transportTcpDataTypeConfiguration: {
        transportTcpDataType: v.dataType
      },
      type: DeviceTransportType.TCP
    };
    if (v.tcpTransportFramingMode === TcpTransportFramingMode.FIXED_LENGTH) {
      configuration.tcpFixedFrameLength = v.tcpFixedFrameLength;
    } else {
      configuration.tcpFixedFrameLength = null;
    }
    this.propagateChange(configuration);
  }
  validate(): ValidationErrors | null {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.valid ? null : {tcpProfileTransport: true};
  }
}
