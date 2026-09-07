///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormArray,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import {
  HttpPullPollDataType,
  MqttPullSubscribeRequest
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-pull-subscribe-requests-config',
  templateUrl: './mqtt-pull-subscribe-requests-config.component.html',
  styleUrls: ['./mqtt-pull-subscribe-requests-config.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttPullSubscribeRequestsConfigComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttPullSubscribeRequestsConfigComponent),
      multi: true
    }
  ]
})
export class MqttPullSubscribeRequestsConfigComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;
  dataTypes = Object.keys(HttpPullPollDataType);
  qosOptions = [0, 1, 2];

  private destroy$ = new Subject<void>();
  private propagateChange: (v: MqttPullSubscribeRequest[]) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  get subscribeRequestsArray(): UntypedFormArray {
    return this.form.get('subscribeRequests') as UntypedFormArray;
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      subscribeRequests: this.fb.array([])
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
    this.disabled = isDisabled;
    this.applyDisabledState();
  }

  writeValue(value: MqttPullSubscribeRequest[] | null): void {
    if (!this.form) {
      return;
    }
    this.subscribeRequestsArray.clear({ emitEvent: false });
    const requests = value?.length ? value : [this.createDefaultRequest()];
    requests.forEach(r => this.subscribeRequestsArray.push(this.createRequestGroup(r), { emitEvent: false }));
    this.applyDisabledState();
  }

  validate(): ValidationErrors | null {
    return this.form.valid && this.subscribeRequestsArray.length > 0 ? null : { subscribeRequests: true };
  }

  addSubscribeRequest(): void {
    this.subscribeRequestsArray.push(this.createRequestGroup(this.createDefaultRequest()));
  }

  removeSubscribeRequest(index: number): void {
    if (this.subscribeRequestsArray.length > 1) {
      this.subscribeRequestsArray.removeAt(index);
    }
  }

  showTelemetryKey(req: UntypedFormGroup): boolean {
    return req.get('dataType')?.value === HttpPullPollDataType.TELEMETRY;
  }

  private createDefaultRequest(): MqttPullSubscribeRequest {
    return {
      id: this.newId(),
      name: `subscribe-${this.subscribeRequestsArray.length + 1}`,
      enabled: true,
      topic: '',
      qos: 1,
      dataType: HttpPullPollDataType.TELEMETRY,
      telemetryPayloadKey: 'mqttPullPayload'
    };
  }

  private createRequestGroup(r: MqttPullSubscribeRequest): UntypedFormGroup {
    return this.fb.group({
      id: [r.id || this.newId()],
      name: [r.name || ''],
      enabled: [r.enabled !== false],
      topic: [r.topic || '', Validators.required],
      qos: [r.qos ?? 1, [Validators.required, Validators.min(0), Validators.max(2)]],
      dataType: [r.dataType || HttpPullPollDataType.TELEMETRY, Validators.required],
      telemetryPayloadKey: [r.telemetryPayloadKey || 'mqttPullPayload']
    });
  }

  private updateModel(): void {
    const requests: MqttPullSubscribeRequest[] = this.subscribeRequestsArray.getRawValue().map((v: any) => ({
      id: v.id,
      name: v.name || undefined,
      enabled: v.enabled,
      topic: v.topic,
      qos: v.qos,
      dataType: v.dataType,
      telemetryPayloadKey: v.telemetryPayloadKey || 'mqttPullPayload'
    }));
    this.propagateChange(requests);
  }

  private applyDisabledState(): void {
    if (!this.form) {
      return;
    }
    if (this.disabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
    }
  }

  private newId(): string {
    return typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `sub-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  }
}
