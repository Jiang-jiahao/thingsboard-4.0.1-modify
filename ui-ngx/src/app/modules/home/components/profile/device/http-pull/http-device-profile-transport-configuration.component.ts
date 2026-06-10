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
  createDeviceProfileTransportConfiguration,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  HttpTransportMode,
  isHttpPullProfileTransportConfiguration,
  resolveHttpProfileTransportTypeForDisplay
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
export class HttpDeviceProfileTransportConfigurationComponent implements OnInit, OnChanges, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;
  /** 档案已保存的 transportType，用于配置 JSON 缺少 type 时恢复工作模式 */
  @Input() entityTransportType: DeviceTransportType;
  /** 父级判定：必须为 HTTP Pull 档案时强制展示主动拉取面板 */
  @Input() httpPullActive = false;

  form: UntypedFormGroup;
  httpTransportMode = HttpTransportMode;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceProfileTransportConfiguration) => void = () => {};
  private pendingValue: DeviceProfileTransportConfiguration | null = null;
  private formReady = false;

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
    this.formReady = true;
    this.applyPendingValue();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.entityTransportType || changes.httpPullActive) {
      this.applyPendingValue();
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

  writeValue(value: DeviceProfileTransportConfiguration): void {
    if (!value) {
      return;
    }
    this.pendingValue = value;
    this.applyPendingValue();
    // 嵌套 CVA（pull/passive 子面板）可能尚未挂载，延迟再应用一次避免误显示被动接收
    setTimeout(() => this.applyPendingValue(), 0);
  }

  private applyPendingValue(): void {
    if (!this.formReady || !this.form || !this.pendingValue) {
      return;
    }
    const value = this.pendingValue;
    const isPull = this.httpPullActive
      || isHttpPullProfileTransportConfiguration(value)
      || resolveHttpProfileTransportTypeForDisplay(this.entityTransportType, value as Record<string, unknown>)
        === DeviceTransportType.HTTP_PULL;
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
      const pull = this.form.get('pullConfiguration').value || {};
      this.propagateChange({
        ...pull,
        type: DeviceTransportType.HTTP_PULL,
        httpTransportMode: HttpTransportMode.PULL
      });
    } else {
      const passive = this.form.get('passiveConfiguration').value || {};
      this.propagateChange({
        type: DeviceTransportType.DEFAULT,
        httpTransportMode: HttpTransportMode.PASSIVE,
        routing: passive.routing
      });
    }
  }
}
