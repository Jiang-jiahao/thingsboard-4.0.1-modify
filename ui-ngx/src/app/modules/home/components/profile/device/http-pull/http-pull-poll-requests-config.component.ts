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
  HTTP_PULL_ROUTING_MODE_OPTIONS,
  HttpPullDeviceIdMatchStrategy,
  HttpPullPollDataType,
  HttpPullPollRequest,
  HttpPullRoutingMode,
  normalizeHttpPullRoutingMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-pull-poll-requests-config',
  templateUrl: './http-pull-poll-requests-config.component.html',
  styleUrls: ['./http-pull-poll-requests-config.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpPullPollRequestsConfigComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpPullPollRequestsConfigComponent),
      multi: true
    }
  ]
})
export class HttpPullPollRequestsConfigComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;
  @Input() defaultQueryingFrequencyMs = 30000;

  form: UntypedFormGroup;
  dataTypes = Object.keys(HttpPullPollDataType);
  httpPullPollDataType = HttpPullPollDataType;
  routingModes = HTTP_PULL_ROUTING_MODE_OPTIONS;
  matchStrategies = Object.keys(HttpPullDeviceIdMatchStrategy);
  httpPullRoutingMode = HttpPullRoutingMode;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: HttpPullPollRequest[]) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  get pollRequestsArray(): UntypedFormArray {
    return this.form.get('pollRequests') as UntypedFormArray;
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      pollRequests: this.fb.array([])
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

  writeValue(value: HttpPullPollRequest[] | null): void {
    if (!this.form) {
      return;
    }
    this.pollRequestsArray.clear({ emitEvent: false });
    const requests = value?.length ? value : [this.createDefaultRequest()];
    requests.forEach(r => this.pollRequestsArray.push(this.createRequestGroup(r), { emitEvent: false }));
    this.applyDisabledState();
  }

  validate(): ValidationErrors | null {
    return this.form.valid && this.pollRequestsArray.length > 0 ? null : { pollRequests: true };
  }

  addPollRequest(): void {
    this.pollRequestsArray.push(this.createRequestGroup(this.createDefaultRequest()));
  }

  removePollRequest(index: number): void {
    if (this.pollRequestsArray.length > 1) {
      this.pollRequestsArray.removeAt(index);
    }
  }

  showRoutingFields(req: UntypedFormGroup): boolean {
    const mode = req.get('routingMode')?.value;
    return mode === HttpPullRoutingMode.MULTI_DEVICE;
  }

  showTelemetryKey(req: UntypedFormGroup): boolean {
    return req.get('dataType')?.value === HttpPullPollDataType.TELEMETRY;
  }

  private createDefaultRequest(): HttpPullPollRequest {
    return {
      id: this.newId(),
      name: `poll-${this.pollRequestsArray.length + 1}`,
      enabled: true,
      pollUrl: '',
      pollMethod: 'GET',
      pollBody: '',
      queryingFrequencyMs: null,
      dataType: HttpPullPollDataType.TELEMETRY,
      requiresAuth: true,
      routing: {
        routingMode: HttpPullRoutingMode.MULTI_DEVICE,
        deviceIdJsonPath: 'deviceId',
        deviceIdMatchStrategy: HttpPullDeviceIdMatchStrategy.DEVICE_NAME,
        telemetryPayloadKey: 'httpPullPayload'
      }
    };
  }

  private createRequestGroup(r: HttpPullPollRequest): UntypedFormGroup {
    const routing = r.routing || {};
    return this.fb.group({
      id: [r.id || this.newId()],
      name: [r.name || ''],
      enabled: [r.enabled !== false],
      requiresAuth: [r.requiresAuth !== false],
      pollUrl: [r.pollUrl || '', Validators.required],
      pollMethod: [r.pollMethod || 'GET', Validators.required],
      pollBody: [r.pollBody || ''],
      queryingFrequencyMs: [r.queryingFrequencyMs ?? null, [Validators.min(1000)]],
      dataType: [r.dataType || HttpPullPollDataType.TELEMETRY, Validators.required],
      routingMode: [normalizeHttpPullRoutingMode(routing.routingMode)],
      responseArrayJsonPath: [routing.responseArrayJsonPath || ''],
      deviceIdJsonPath: [routing.deviceIdJsonPath || 'deviceId'],
      deviceIdMatchStrategy: [routing.deviceIdMatchStrategy || HttpPullDeviceIdMatchStrategy.DEVICE_NAME],
      targetDeviceProfileId: [routing.targetDeviceProfileId || ''],
      telemetryPayloadKey: [routing.telemetryPayloadKey || 'httpPullPayload']
    });
  }

  private updateModel(): void {
    const requests: HttpPullPollRequest[] = this.pollRequestsArray.getRawValue().map((v: any) => ({
      id: v.id,
      name: v.name || undefined,
      enabled: v.enabled,
      requiresAuth: v.requiresAuth,
      pollUrl: v.pollUrl,
      pollMethod: v.pollMethod,
      pollBody: v.pollBody || undefined,
      queryingFrequencyMs: v.queryingFrequencyMs || undefined,
      dataType: v.dataType,
      routing: {
        routingMode: v.routingMode,
        responseArrayJsonPath: v.responseArrayJsonPath || undefined,
        deviceIdJsonPath: v.deviceIdJsonPath,
        deviceIdMatchStrategy: v.deviceIdMatchStrategy,
        targetDeviceProfileId: v.targetDeviceProfileId || undefined,
        telemetryPayloadKey: v.telemetryPayloadKey
      }
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
      : `poll-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  }
}
