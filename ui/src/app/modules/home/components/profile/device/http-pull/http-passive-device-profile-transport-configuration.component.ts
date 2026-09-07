///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validator
} from '@angular/forms';
import {
  DefaultDeviceProfileTransportConfiguration,
  DeviceTransportType,
  HttpPullDeviceRoutingConfiguration,
  HttpPullRoutingMode,
  HttpTransportMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-passive-device-profile-transport-configuration',
  templateUrl: './http-passive-device-profile-transport-configuration.component.html',
  styleUrls: ['./http-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpPassiveDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpPassiveDeviceProfileTransportConfigurationComponent),
      multi: true
    }
  ]
})
export class HttpPassiveDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DefaultDeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      routing: [null as HttpPullDeviceRoutingConfiguration | null]
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }

  registerOnTouched(_fn: any): void {}

  setDisabledState(isDisabled: boolean): void {
    if (isDisabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
    }
  }

  writeValue(value: DefaultDeviceProfileTransportConfiguration | null): void {
    this.form.patchValue({
      routing: value?.routing || null
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return null;
  }

  private updateModel(): void {
    const routing = this.form.get('routing').value as HttpPullDeviceRoutingConfiguration | null;
    const model: DefaultDeviceProfileTransportConfiguration = {
      type: DeviceTransportType.DEFAULT,
      httpTransportMode: HttpTransportMode.PASSIVE
    };
    if (routing?.routingMode === HttpPullRoutingMode.MULTI_DEVICE) {
      model.routing = routing;
    }
    this.propagateChange(model);
  }
}
