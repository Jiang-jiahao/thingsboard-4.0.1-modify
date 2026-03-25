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
import { DeviceTransportType, TcpDeviceTransportConfiguration } from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
@Component({
  selector: 'tb-tcp-device-transport-configuration',
  templateUrl: './tcp-device-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => TcpDeviceTransportConfigurationComponent),
      multi: true
    }, {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => TcpDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class TcpDeviceTransportConfigurationComponent implements ControlValueAccessor, OnInit, Validator {
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
      host: ['127.0.0.1'],
      port: [5025, [Validators.min(1), Validators.max(65535)]],
      sourceHost: [''],
      serverBindPort: [null, [Validators.min(1), Validators.max(65535)]]
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
  }
  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }
  writeValue(value: TcpDeviceTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue({
        host: value.host,
        port: value.port,
        sourceHost: value.sourceHost || '',
        serverBindPort: value.serverBindPort
      }, {emitEvent: false});
    }
  }
  validate(): ValidationErrors | null {
    return this.tcpDeviceTransportConfigurationFormGroup.valid ? null : {tcpDeviceTransport: false};
  }
  private updateModel() {
    if (!this.tcpDeviceTransportConfigurationFormGroup.valid) {
        this.propagateChange(null);
        return;
      }
      const v = this.tcpDeviceTransportConfigurationFormGroup.getRawValue();
      const configuration: TcpDeviceTransportConfiguration = {
        type: DeviceTransportType.TCP,
        host: v.host,
        port: v.port
      };
      const sh = v.sourceHost?.trim();
      if (sh) {
        configuration.sourceHost = sh;
      }
      if (v.serverBindPort != null && v.serverBindPort !== '') {
        configuration.serverBindPort = Number(v.serverBindPort);
    }
    this.propagateChange(configuration);
  }
}
