///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { ChangeDetectorRef, Component, forwardRef, OnDestroy, OnInit } from '@angular/core';
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
  DeviceTransportConfiguration,
  DeviceTransportType,
  MqttPullAuthType,
  MqttPullDeviceTransportConfiguration
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-pull-device-transport-configuration',
  templateUrl: './mqtt-pull-device-transport-configuration.component.html',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttPullDeviceTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttPullDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class MqttPullDeviceTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  form: UntypedFormGroup;
  authTypes = Object.keys(MqttPullAuthType);

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceTransportConfiguration) => void = () => {};
  private pendingValue: MqttPullDeviceTransportConfiguration | null = null;
  private valueReady = false;

  constructor(private fb: UntypedFormBuilder,
              private cd: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      brokerUrl: ['', Validators.required],
      clientId: [''],
      topicPrefix: [''],
      externalDeviceId: [''],
      authType: [MqttPullAuthType.NONE],
      username: [''],
      password: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.applyPendingValue();
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
    if (!this.form) {
      return;
    }
    if (isDisabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
      this.applyPendingValue();
    }
  }

  writeValue(value: DeviceTransportConfiguration | null): void {
    if (!value) {
      return;
    }
    const cfg = value as MqttPullDeviceTransportConfiguration;
    const auth = cfg.auth || {};
    this.pendingValue = {
      ...cfg,
      brokerUrl: (cfg.brokerUrl || '').trim(),
      clientId: (cfg.clientId || '').trim(),
      topicPrefix: (cfg.topicPrefix || '').trim(),
      externalDeviceId: (cfg.externalDeviceId || '').trim(),
      auth: {
        authType: auth.authType || MqttPullAuthType.NONE,
        username: auth.username,
        password: auth.password
      }
    };
    this.applyPendingValue();
  }

  validate(): ValidationErrors | null {
    return this.form?.valid ? null : { mqttPullDevice: true };
  }

  private applyPendingValue(): void {
    if (!this.form || !this.pendingValue) {
      return;
    }
    const auth = this.pendingValue.auth || {};
    this.valueReady = false;
    this.form.patchValue({
      brokerUrl: this.pendingValue.brokerUrl || '',
      clientId: this.pendingValue.clientId || '',
      topicPrefix: this.pendingValue.topicPrefix || '',
      externalDeviceId: this.pendingValue.externalDeviceId || '',
      authType: auth.authType || MqttPullAuthType.NONE,
      username: auth.username || '',
      password: auth.password || ''
    }, { emitEvent: false });
    this.valueReady = true;
    this.updateModel();
    this.cd.markForCheck();
  }

  private updateModel(): void {
    const v = this.form.getRawValue();
    const authType = v.authType || MqttPullAuthType.NONE;
    this.propagateChange({
      brokerUrl: (v.brokerUrl || '').trim() || undefined,
      clientId: (v.clientId || '').trim() || undefined,
      topicPrefix: (v.topicPrefix || '').trim() || undefined,
      externalDeviceId: (v.externalDeviceId || '').trim() || undefined,
      auth: {
        authType,
        username: authType === MqttPullAuthType.USERNAME_PASSWORD ? (v.username || undefined) : undefined,
        password: authType === MqttPullAuthType.USERNAME_PASSWORD ? (v.password || undefined) : undefined
      },
      type: DeviceTransportType.MQTT_PULL
    });
  }
}
