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
  createDeviceProfileTransportConfiguration,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  HttpTransportMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-device-profile-transport-configuration',
  templateUrl: './http-device-profile-transport-configuration.component.html',
  styleUrls: ['./http-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpDeviceProfileTransportConfigurationComponent),
      multi: true
    }
  ]
})
export class HttpDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;
  httpTransportMode = HttpTransportMode;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      httpMode: [HttpTransportMode.PULL],
      pullConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.HTTP_PULL)],
      passiveConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.DEFAULT)]
    });
    this.form.get('httpMode').valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
    this.form.get('pullConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('httpMode').value === HttpTransportMode.PULL)
    ).subscribe(() => this.updateModel());
    this.form.get('passiveConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('httpMode').value === HttpTransportMode.PASSIVE)
    ).subscribe(() => this.updateModel());
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

  writeValue(value: DeviceProfileTransportConfiguration): void {
    if (!value) {
      return;
    }
    const isPull = value.type === DeviceTransportType.HTTP_PULL
      || (value as { pollUrl?: string }).pollUrl != null;
    this.form.patchValue({
      httpMode: isPull ? HttpTransportMode.PULL : HttpTransportMode.PASSIVE,
      pullConfiguration: isPull ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.HTTP_PULL),
      passiveConfiguration: !isPull ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.DEFAULT)
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    if (this.form.get('httpMode').value === HttpTransportMode.PULL) {
      return this.form.get('pullConfiguration').valid ? null : { httpPull: true };
    }
    return null;
  }

  private updateModel(): void {
    const mode = this.form.get('httpMode').value;
    if (mode === HttpTransportMode.PULL) {
      this.propagateChange(this.form.get('pullConfiguration').value);
    } else {
      const passive = this.form.get('passiveConfiguration').value || {};
      this.propagateChange({ ...passive, type: DeviceTransportType.DEFAULT });
    }
  }
}
