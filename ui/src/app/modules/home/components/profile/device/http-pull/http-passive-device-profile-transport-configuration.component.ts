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
  DefaultDeviceProfileTransportConfiguration,
  DeviceTransportType,
  HttpTransportMode
} from '@shared/models/device.models';

@Component({
  selector: 'jnks-iot-http-passive-device-profile-transport-configuration',
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
export class HttpPassiveDeviceProfileTransportConfigurationComponent implements OnInit, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;

  private propagateChange: (v: DefaultDeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({});
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

  /**
   * 本组件没有任何可编辑字段，被动模式的配置是个常量，所以**只在父级还没有值时补一次默认值**。
   * <p>
   * 不能无条件回写：CVA 调 `propagateChange(...)` 会让 Angular 把控件标脏，于是刚打开档案页、
   * 什么都没编辑，顶层表单就已经是 `ng-dirty`，离开时会弹「有未保存的更改」。
   */
  writeValue(value: DefaultDeviceProfileTransportConfiguration | null): void {
    if (value == null) {
      this.propagateChange({
        type: DeviceTransportType.DEFAULT,
        httpTransportMode: HttpTransportMode.PASSIVE
      });
    }
  }

  validate(): ValidationErrors | null {
    return null;
  }
}
