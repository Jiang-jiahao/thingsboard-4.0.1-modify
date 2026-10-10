import { Component, forwardRef, Input, OnInit } from '@angular/core';
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
  DeviceTransportType
} from '@shared/models/device.models';

@Component({
  selector: 'jnks-iot-http-passive-device-transport-configuration',
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
export class HttpPassiveDeviceTransportConfigurationComponent implements OnInit, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;

  private propagateChange: (v: DefaultDeviceTransportConfiguration) => void = () => {};
  private formReady = false;

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({});
    this.formReady = true;
    this.updateModel();
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

  writeValue(_value: DefaultDeviceTransportConfiguration | null): void {
    if (this.formReady) {
      this.updateModel();
    }
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPassiveDevice: true };
  }

  private updateModel(): void {
    this.propagateChange({ type: DeviceTransportType.DEFAULT });
  }
}
