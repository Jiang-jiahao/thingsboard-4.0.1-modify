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
  HttpPullAuthConfiguration,
  HttpPullAuthType,
  HttpPullDeviceProfileTransportConfiguration,
  HttpPullPollDataType,
  HttpPullPollRequest,
  HttpTransportMode
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'jnks-iot-http-pull-device-profile-transport-configuration',
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

  constructor(private fb: UntypedFormBuilder) {}

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
      loginHeadersJson: ['{}'],
      accessTokenJsonPath: ['$.token'],
      tokenHeader: ['Authorization'],
      tokenPrefix: ['Bearer '],
      defaultTokenTtlSec: [3600, [Validators.min(1)]],
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
        telemetryPayloadKey: value.routing?.telemetryPayloadKey || 'httpPullPayload'
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
      loginHeadersJson: auth.loginHeaders && Object.keys(auth.loginHeaders).length ? JSON.stringify(auth.loginHeaders) : '{}',
      accessTokenJsonPath: auth.accessTokenJsonPath,
      tokenHeader: auth.tokenHeader ?? 'Authorization',
      tokenPrefix: auth.tokenPrefix ?? 'Bearer ',
      defaultTokenTtlSec: auth.defaultTokenTtlSec ?? 3600,
      tokenUrl: auth.tokenUrl,
      clientId: auth.clientId,
      clientSecret: auth.clientSecret,
      oauthUsername: auth.oauthUsername,
      oauthPassword: auth.oauthPassword
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    if (!this.form.valid) {
      return { httpPull: true };
    }
    if (parseJsonObject(this.form.get('loginHeadersJson').value) === null) {
      return { loginHeadersJson: true };
    }
    return null;
  }

  private updateModel(): void {
    const v = this.form.value;
    const auth: HttpPullAuthConfiguration = {
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
      // 空串表示「不加前缀」，不能回退成 Bearer
      tokenHeader: v.tokenHeader || undefined,
      tokenPrefix: v.tokenPrefix,
      defaultTokenTtlSec: v.defaultTokenTtlSec,
      tokenUrl: v.tokenUrl,
      clientId: v.clientId,
      clientSecret: v.clientSecret,
      oauthUsername: v.oauthUsername,
      oauthPassword: v.oauthPassword
    };
    const loginHeaders = parseJsonObject(v.loginHeadersJson);
    if (loginHeaders) {
      auth.loginHeaders = loginHeaders;
    }
    const model: HttpPullDeviceProfileTransportConfiguration & { type: DeviceTransportType } = {
      type: DeviceTransportType.HTTP_PULL,
      httpTransportMode: HttpTransportMode.PULL,
      timeoutMs: v.timeoutMs,
      readTimeoutMs: v.readTimeoutMs,
      queryingFrequencyMs: v.queryingFrequencyMs,
      pollRequests: v.pollRequests,
      auth
    };
    this.propagateChange(model);
  }
}

function parseJsonObject(text: string): Record<string, string> | null {
  const t = (text ?? '').trim();
  if (!t || t === '{}') {
    return {};
  }
  try {
    const v = JSON.parse(t);
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const out: Record<string, string> = {};
      for (const k of Object.keys(v)) {
        out[k] = String(v[k]);
      }
      return out;
    }
    return null;
  } catch {
    return null;
  }
}
