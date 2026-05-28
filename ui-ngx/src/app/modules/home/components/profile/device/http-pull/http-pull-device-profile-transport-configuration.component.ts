///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { HttpPullRoutingHelpDialogComponent } from './http-pull-routing-help-dialog.component';
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
  HttpPullAuthType,
  HttpPullDeviceIdMatchStrategy,
  HttpPullDeviceProfileTransportConfiguration,
  HttpPullRoutingMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-pull-device-profile-transport-configuration',
  templateUrl: './http-pull-device-profile-transport-configuration.component.html',
  styleUrls: ['./http-pull-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpPullDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpPullDeviceProfileTransportConfigurationComponent),
      multi: true
    }]
})
export class HttpPullDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;
  httpPullAuthType = HttpPullAuthType;
  httpPullRoutingMode = HttpPullRoutingMode;
  httpPullMatchStrategy = HttpPullDeviceIdMatchStrategy;
  authTypes = Object.keys(HttpPullAuthType);
  routingModes = Object.keys(HttpPullRoutingMode);
  matchStrategies = Object.keys(HttpPullDeviceIdMatchStrategy);

  private destroy$ = new Subject<void>();
  private propagateChange: (v: HttpPullDeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder,
              private dialog: MatDialog) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      timeoutMs: [10000, [Validators.required, Validators.min(0)]],
      readTimeoutMs: [10000, [Validators.required, Validators.min(0)]],
      queryingFrequencyMs: [30000, [Validators.required, Validators.min(1000)]],
      pollUrl: ['', Validators.required],
      pollMethod: ['GET', Validators.required],
      pollBody: [''],
      authType: [HttpPullAuthType.NONE],
      apiKeyHeader: ['X-API-Key'],
      apiKeyValue: [''],
      apiKeyInQuery: [false],
      username: [''],
      password: [''],
      bearerToken: [''],
      loginUrl: [''],
      loginBody: [''],
      accessTokenJsonPath: ['$.token'],
      tokenUrl: [''],
      clientId: [''],
      clientSecret: [''],
      oauthUsername: [''],
      oauthPassword: [''],
      routingMode: [HttpPullRoutingMode.SINGLE_DEVICE],
      responseArrayJsonPath: [''],
      deviceIdJsonPath: ['deviceId'],
      deviceIdMatchStrategy: [HttpPullDeviceIdMatchStrategy.DEVICE_NAME],
      targetDeviceProfileId: [''],
      telemetryPayloadKey: ['httpPullPayload']
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

  writeValue(value: HttpPullDeviceProfileTransportConfiguration): void {
    if (!value) {
      return;
    }
    const auth = value.auth || {};
    const routing = value.routing || {};
    this.form.patchValue({
      timeoutMs: value.timeoutMs,
      readTimeoutMs: value.readTimeoutMs,
      queryingFrequencyMs: value.queryingFrequencyMs,
      pollUrl: value.pollUrl,
      pollMethod: value.pollMethod,
      pollBody: value.pollBody,
      authType: auth.authType || HttpPullAuthType.NONE,
      apiKeyHeader: auth.apiKeyHeader,
      apiKeyValue: auth.apiKeyValue,
      apiKeyInQuery: auth.apiKeyInQuery,
      username: auth.username,
      password: auth.password,
      bearerToken: auth.bearerToken,
      loginUrl: auth.loginUrl,
      loginBody: auth.loginBody,
      accessTokenJsonPath: auth.accessTokenJsonPath,
      tokenUrl: auth.tokenUrl,
      clientId: auth.clientId,
      clientSecret: auth.clientSecret,
      oauthUsername: auth.oauthUsername,
      oauthPassword: auth.oauthPassword,
      routingMode: routing.routingMode || HttpPullRoutingMode.SINGLE_DEVICE,
      responseArrayJsonPath: routing.responseArrayJsonPath,
      deviceIdJsonPath: routing.deviceIdJsonPath,
      deviceIdMatchStrategy: routing.deviceIdMatchStrategy,
      targetDeviceProfileId: routing.targetDeviceProfileId,
      telemetryPayloadKey: routing.telemetryPayloadKey
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPull: true };
  }

  private updateModel(): void {
    const v = this.form.value;
    const model: HttpPullDeviceProfileTransportConfiguration & { type: DeviceTransportType } = {
      type: DeviceTransportType.HTTP_PULL,
      timeoutMs: v.timeoutMs,
      readTimeoutMs: v.readTimeoutMs,
      queryingFrequencyMs: v.queryingFrequencyMs,
      pollUrl: v.pollUrl,
      pollMethod: v.pollMethod,
      pollBody: v.pollBody || undefined,
      auth: {
        authType: v.authType,
        apiKeyHeader: v.apiKeyHeader,
        apiKeyValue: v.apiKeyValue,
        apiKeyInQuery: v.apiKeyInQuery,
        username: v.username,
        password: v.password,
        bearerToken: v.bearerToken,
        loginUrl: v.loginUrl,
        loginBody: v.loginBody,
        accessTokenJsonPath: v.accessTokenJsonPath,
        tokenUrl: v.tokenUrl,
        clientId: v.clientId,
        clientSecret: v.clientSecret,
        oauthUsername: v.oauthUsername,
        oauthPassword: v.oauthPassword
      },
      routing: {
        routingMode: v.routingMode,
        responseArrayJsonPath: v.responseArrayJsonPath || undefined,
        deviceIdJsonPath: v.deviceIdJsonPath,
        deviceIdMatchStrategy: v.deviceIdMatchStrategy,
        targetDeviceProfileId: v.targetDeviceProfileId || undefined,
        telemetryPayloadKey: v.telemetryPayloadKey
      }
    };
    this.propagateChange(model);
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
}
