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
  isMqttPullProfileTransportConfiguration,
  MqttTransportMode,
  resolveMqttProfileTransportTypeForDisplay
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-device-profile-transport-configuration',
  templateUrl: './mqtt-device-profile-transport-configuration.component.html',
  styleUrls: ['./mqtt-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttDeviceProfileTransportConfigurationComponent),
      multi: true
    }
  ]
})
export class MqttDeviceProfileTransportConfigurationComponent implements OnInit, OnChanges, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;
  @Input() entityTransportType: DeviceTransportType;
  @Input() mqttPullActive = false;

  form: UntypedFormGroup;
  mqttTransportMode = MqttTransportMode;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceProfileTransportConfiguration) => void = () => {};
  private pendingValue: DeviceProfileTransportConfiguration | null = null;
  private formReady = false;

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      mqttMode: [MqttTransportMode.PULL],
      pullConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.MQTT_PULL)],
      passiveConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.MQTT)]
    });
    this.form.get('mqttMode').valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
    this.form.get('pullConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('mqttMode').value === MqttTransportMode.PULL)
    ).subscribe(() => this.updateModel());
    this.form.get('passiveConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('mqttMode').value === MqttTransportMode.PASSIVE)
    ).subscribe(() => this.updateModel());
    this.formReady = true;
    this.applyPendingValue();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.entityTransportType || changes.mqttPullActive) {
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
    setTimeout(() => this.applyPendingValue(), 0);
  }

  private applyPendingValue(): void {
    if (!this.formReady || !this.form || !this.pendingValue) {
      return;
    }
    const value = this.pendingValue;
    const isPull = this.resolveIsPullMode(value);
    this.form.patchValue({
      mqttMode: isPull ? MqttTransportMode.PULL : MqttTransportMode.PASSIVE,
      pullConfiguration: isPull ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.MQTT_PULL),
      passiveConfiguration: !isPull ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.MQTT)
    }, { emitEvent: false });
  }

  private resolveIsPullMode(value: DeviceProfileTransportConfiguration): boolean {
    if (value.mqttTransportMode === MqttTransportMode.PULL) {
      return true;
    }
    if (value.mqttTransportMode === MqttTransportMode.PASSIVE) {
      return false;
    }
    return this.mqttPullActive
      || isMqttPullProfileTransportConfiguration(value)
      || resolveMqttProfileTransportTypeForDisplay(this.entityTransportType, value as Record<string, unknown>)
        === DeviceTransportType.MQTT_PULL;
  }

  validate(): ValidationErrors | null {
    if (this.form.get('mqttMode').value === MqttTransportMode.PULL) {
      return this.form.get('pullConfiguration').valid ? null : { mqttPull: true };
    }
    return null;
  }

  private updateModel(): void {
    const mode = this.form.get('mqttMode').value;
    if (mode === MqttTransportMode.PULL) {
      const pull = this.form.get('pullConfiguration').value || {};
      const configuration: DeviceProfileTransportConfiguration = {
        ...pull,
        type: DeviceTransportType.MQTT_PULL,
        mqttTransportMode: MqttTransportMode.PULL
      };
      this.pendingValue = configuration;
      this.propagateChange(configuration);
    } else {
      const passive = this.form.get('passiveConfiguration').value || {};
      const configuration: DeviceProfileTransportConfiguration = {
        ...passive,
        type: DeviceTransportType.MQTT,
        mqttTransportMode: MqttTransportMode.PASSIVE
      };
      this.pendingValue = configuration;
      this.propagateChange(configuration);
    }
  }
}
