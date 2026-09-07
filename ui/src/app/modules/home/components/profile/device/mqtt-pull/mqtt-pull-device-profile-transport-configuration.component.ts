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
  Validator,
  Validators
} from '@angular/forms';
import {
  DeviceTransportType,
  MqttPullDeviceProfileTransportConfiguration,
  MqttPullSubscribeRequest,
  MqttTransportMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-pull-device-profile-transport-configuration',
  templateUrl: './mqtt-pull-device-profile-transport-configuration.component.html',
  styleUrls: ['./mqtt-pull-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttPullDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttPullDeviceProfileTransportConfigurationComponent),
      multi: true
    }]
})
export class MqttPullDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: MqttPullDeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      connectTimeoutMs: [10000, [Validators.required, Validators.min(0)]],
      keepAliveSec: [60, [Validators.required, Validators.min(1)]],
      cleanSession: [true],
      reconnectIntervalMs: [5000, [Validators.required, Validators.min(1000)]],
      subscribeRequests: [[] as MqttPullSubscribeRequest[], Validators.required]
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

  writeValue(value: MqttPullDeviceProfileTransportConfiguration): void {
    if (!value) {
      return;
    }
    this.form.patchValue({
      connectTimeoutMs: value.connectTimeoutMs,
      keepAliveSec: value.keepAliveSec,
      cleanSession: value.cleanSession !== false,
      reconnectIntervalMs: value.reconnectIntervalMs,
      subscribeRequests: value.subscribeRequests?.length ? value.subscribeRequests : undefined
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { mqttPull: true };
  }

  private updateModel(): void {
    const v = this.form.value;
    const model: MqttPullDeviceProfileTransportConfiguration & { type: DeviceTransportType } = {
      type: DeviceTransportType.MQTT_PULL,
      mqttTransportMode: MqttTransportMode.PULL,
      connectTimeoutMs: v.connectTimeoutMs,
      keepAliveSec: v.keepAliveSec,
      cleanSession: v.cleanSession,
      reconnectIntervalMs: v.reconnectIntervalMs,
      subscribeRequests: v.subscribeRequests
    };
    this.propagateChange(model);
  }
}
