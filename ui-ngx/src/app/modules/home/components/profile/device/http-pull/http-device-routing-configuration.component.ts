///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { HttpPullRoutingHelpDialogComponent } from './http-pull-routing-help-dialog.component';
import {
  ControlValueAccessor,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormGroup,
  Validators
} from '@angular/forms';
import {
  HTTP_PULL_ROUTING_MODE_OPTIONS,
  HttpPullDeviceIdMatchStrategy,
  HttpPullDeviceRoutingConfiguration,
  HttpPullRoutingMode,
  normalizeHttpPullRoutingMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-device-routing-configuration',
  templateUrl: './http-device-routing-configuration.component.html',
  styleUrls: ['./http-pull-device-profile-transport-configuration.component.scss'],
  providers: [{
    provide: NG_VALUE_ACCESSOR,
    useExisting: forwardRef(() => HttpDeviceRoutingConfigurationComponent),
    multi: true
  }]
})
export class HttpDeviceRoutingConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor {

  @Input() disabled: boolean;
  @Input() translationPrefix = 'device-profile.http-push';

  private static readonly DEFAULT_TELEMETRY_KEY = 'httpPushPayload';

  form: UntypedFormGroup;
  httpPullRoutingMode = HttpPullRoutingMode;
  routingModes = HTTP_PULL_ROUTING_MODE_OPTIONS;
  matchStrategies = Object.keys(HttpPullDeviceIdMatchStrategy);

  private destroy$ = new Subject<void>();
  private propagateChange: (v: HttpPullDeviceRoutingConfiguration | null) => void = () => {};

  constructor(private fb: UntypedFormBuilder,
              private dialog: MatDialog) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      routingMode: [HttpPullRoutingMode.SINGLE_DEVICE],
      responseArrayJsonPath: [''],
      deviceIdJsonPath: ['deviceId'],
      deviceIdMatchStrategy: [HttpPullDeviceIdMatchStrategy.DEVICE_NAME],
      targetDeviceProfileId: ['']
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

  writeValue(value: HttpPullDeviceRoutingConfiguration | null): void {
    const routing = value || {};
    this.form.patchValue({
      routingMode: normalizeHttpPullRoutingMode(routing.routingMode),
      responseArrayJsonPath: routing.responseArrayJsonPath || '',
      deviceIdJsonPath: routing.deviceIdJsonPath || 'deviceId',
      deviceIdMatchStrategy: routing.deviceIdMatchStrategy || HttpPullDeviceIdMatchStrategy.DEVICE_NAME,
      targetDeviceProfileId: routing.targetDeviceProfileId || ''
    }, { emitEvent: false });
  }

  openRoutingExample(event: Event): void {
    event.stopPropagation();
    this.dialog.open(HttpPullRoutingHelpDialogComponent, {
      panelClass: ['tb-dialog', 'tb-fullscreen-dialog'],
      autoFocus: false,
      width: '560px',
      maxWidth: '95vw'
    });
  }

  private updateModel(): void {
    const v = this.form.value;
    const routing: HttpPullDeviceRoutingConfiguration = {
      routingMode: v.routingMode,
      responseArrayJsonPath: v.responseArrayJsonPath || undefined,
      deviceIdJsonPath: v.deviceIdJsonPath,
      deviceIdMatchStrategy: v.deviceIdMatchStrategy,
      targetDeviceProfileId: v.targetDeviceProfileId || undefined,
      telemetryPayloadKey: HttpDeviceRoutingConfigurationComponent.DEFAULT_TELEMETRY_KEY
    };
    this.propagateChange(routing);
  }
}
