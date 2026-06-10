///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
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
  DefaultDeviceTransportConfiguration,
  DeviceTransportType,
  HttpPullRoutingMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-passive-device-transport-configuration',
  templateUrl: './http-passive-device-transport-configuration.component.html',
  styleUrls: ['./http-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpPassiveDeviceTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpPassiveDeviceTransportConfigurationComponent),
      multi: true
    }
  ]
})
export class HttpPassiveDeviceTransportConfigurationComponent implements OnInit, OnDestroy, OnChanges, ControlValueAccessor, Validator {

  @Input() disabled: boolean;
  @Input() httpPushRoutingMode: HttpPullRoutingMode | null = null;

  form: UntypedFormGroup;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DefaultDeviceTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  get singleDeviceMode(): boolean {
    return this.httpPushRoutingMode === HttpPullRoutingMode.SINGLE_DEVICE
      || this.httpPushRoutingMode == null;
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      gateway: [true],
      externalDeviceId: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.httpPushRoutingMode && this.form) {
      this.updateModel();
    }
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

  writeValue(value: DefaultDeviceTransportConfiguration | null): void {
    this.form.patchValue({
      gateway: value?.gateway !== false,
      externalDeviceId: value?.externalDeviceId || ''
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPassiveDevice: true };
  }

  private updateModel(): void {
    const v = this.form.value;
    this.propagateChange({
      type: DeviceTransportType.DEFAULT,
      gateway: v.gateway,
      externalDeviceId: v.externalDeviceId || undefined
    });
  }
}
