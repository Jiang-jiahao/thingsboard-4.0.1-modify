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
  DeviceData,
  DeviceProfileInfo,
  deviceProfileTypeConfigurationInfoMap,
  deviceTransportTypeConfigurationInfoMap
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
    if (changes.deviceProfile && this.deviceDataFormGroup) {
      this.applyVisibilityFromValue(this.lastWrittenValue);
    }
  }

  writeValue(value: DeviceData | null): void {
    this.lastWrittenValue = value;
    this.applyVisibilityFromValue(value);
    this.deviceDataFormGroup.patchValue({configuration: value?.configuration}, {emitEvent: false});
    this.deviceDataFormGroup.patchValue({transportConfiguration: value?.transportConfiguration}, {emitEvent: false});
  }

  private applyVisibilityFromValue(value: DeviceData | null): void {
    const profileType = value?.configuration?.type
      ?? (value != null ? this.deviceProfile?.type : undefined);
    const profileInfo = profileType && deviceProfileTypeConfigurationInfoMap.get(profileType);
    this.displayDeviceConfiguration = !!(profileInfo?.hasDeviceConfiguration);
    const transportType = value?.transportConfiguration?.type
    ?? (value != null ? this.deviceProfile?.transportType : undefined);
    const transportInfo = transportType && deviceTransportTypeConfigurationInfoMap.get(transportType);
    this.displayTransportConfiguration = !!(transportInfo?.hasDeviceConfiguration);
  }

  validate(): ValidationErrors | null {
    return this.deviceDataFormGroup.valid ? null : {
      deviceDataForm: false
    };
  }

  private updateModel() {
    let deviceData: DeviceData = null;
    if (this.deviceDataFormGroup.valid) {
      deviceData = this.deviceDataFormGroup.getRawValue();
    }
    this.propagateChange(deviceData);
  }
}
