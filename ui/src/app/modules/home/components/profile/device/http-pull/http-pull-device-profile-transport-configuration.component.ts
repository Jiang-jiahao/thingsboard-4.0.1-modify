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
  HttpPullDeviceProfileTransportConfiguration,
  HttpPullPollDataType,
  HttpPullPollRequest,
  HttpTransportMode
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
  authTypes = Object.keys(HttpPullAuthType);

  private destroy$ = new Subject<void>();
  private propagateChange: (v: HttpPullDeviceProfileTransportConfiguration) => void = () => {};

  constructor(private fb: UntypedFormBuilder,
              private dialog: MatDialog) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      timeoutMs: [10000, [Validators.required, Validators.min(0)]],
      readTimeoutMs: [10000, [Validators.required, Validators.min(0)]],
      queryingFrequencyMs: [30000, [Validators.required, Validators.min(1000)]],
      pollRequests: [[] as HttpPullPollRequest[], Validators.required],
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
      oauthPassword: ['']
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
    let pollRequests = value.pollRequests;
    if (!pollRequests?.length && value.pollUrl) {
      pollRequests = [{
        id: 'legacy',
        name: 'poll-1',
        enabled: true,
        pollUrl: value.pollUrl,
        pollMethod: value.pollMethod || 'GET',
        pollBody: value.pollBody,
        queryingFrequencyMs: value.queryingFrequencyMs,
        dataType: HttpPullPollDataType.TELEMETRY,
        requiresAuth: true,
        routing: value.routing
      }];
    }
    this.form.patchValue({
      timeoutMs: value.timeoutMs,
      readTimeoutMs: value.readTimeoutMs,
      queryingFrequencyMs: value.queryingFrequencyMs,
      pollRequests: pollRequests?.length ? pollRequests : undefined,
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
      oauthPassword: auth.oauthPassword
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPull: true };
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
    const model: HttpPullDeviceProfileTransportConfiguration & { type: DeviceTransportType } = {
      type: DeviceTransportType.HTTP_PULL,
      httpTransportMode: HttpTransportMode.PULL,
      timeoutMs: v.timeoutMs,
      readTimeoutMs: v.readTimeoutMs,
      queryingFrequencyMs: v.queryingFrequencyMs,
      pollRequests: v.pollRequests,
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
      }
    };
    this.propagateChange(model);
  }
}
