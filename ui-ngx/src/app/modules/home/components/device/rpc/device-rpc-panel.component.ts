///

/// Copyright © 2016-2025 The Thingsboard Authors

///



import { ChangeDetectorRef, Component, DestroyRef, Input, OnChanges, SimpleChanges } from '@angular/core';

import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';

import { Store } from '@ngrx/store';

import { AppState } from '@core/core.state';

import { DeviceService } from '@core/http/device.service';

import { DeviceProfileService } from '@core/http/device-profile.service';

import { ProtocolTemplateBundleService } from '@core/services/protocol-template-bundle.service';

import { ActionNotificationShow } from '@core/notification/notification.actions';

import { TranslateService } from '@ngx-translate/core';

import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

import { defaultIfEmpty, finalize, map, of, switchMap } from 'rxjs';

import {

  Device,

  DeviceInfo,

  DeviceProfileRpcBindingType,

  DeviceProfileRpcMethod,

  DeviceTransportType,

  isTcpHexVariableByteSlice,

  ProtocolTemplateBundle,

  TcpDeviceProfileTransportConfiguration,
  UdpDeviceProfileTransportConfiguration,
  isProtocolTemplateWireTransport

} from '@shared/models/device.models';

import {

  applyDeviceRpcParamDefaults,

  applyParamMapToTemplateValues,

  buildInvokeFieldRows,

  catalogMethodFieldMetaMap,

  catalogMethodLabel,

  catalogMethodPlatformKeys,

  defaultsFromFixedEntries,

  DeviceRpcFixedParamEntry,

  DeviceRpcInvokeFieldRow,

  DeviceRpcMethodFieldMeta,

  findBundleCommand,

  fixedEntriesFromDefaults,

  mergeNativeParams,

  mergeRpcPlatformValues,

  parseNativeParamsJson,

  protocolTemplateBundleIdFromTcpProfile,
  protocolTemplateBundleIdFromWireProfile,
  isHttpOutboundRpcBinding,
  isMqttCustomRpcBinding,
  isProtocolTemplateRpcBinding,

  resolveMethodRpcDefaults,

  rpcParamDefaultsByMethodFromDeviceData,

  templateFieldKeyForPlatformKey

} from './device-profile-rpc-invoke.util';

import {

  formatDownlinkFieldEcho,

  parseDownlinkFieldInput

} from '@home/pages/profiles/protocol-template-downlink-fields.util';



@Component({

  selector: 'tb-device-rpc-panel',

  templateUrl: './device-rpc-panel.component.html',

  styleUrls: ['./device-rpc-panel.component.scss']

})

export class DeviceRpcPanelComponent implements OnChanges {



  @Input() active = false;

  @Input() device: DeviceInfo | Device | null;



  readonly DeviceProfileRpcBindingType = DeviceProfileRpcBindingType;



  loading = false;

  catalogMethods: DeviceProfileRpcMethod[] = [];

  transportType: DeviceTransportType | null = null;

  bundleId: string | null = null;

  bundle: ProtocolTemplateBundle | null = null;



  invokeForm: UntypedFormGroup;

  selectedMethod: DeviceProfileRpcMethod | null = null;

  get selectedMethodUsesProtocolTemplate(): boolean {
    return isProtocolTemplateRpcBinding(this.selectedMethod?.bindingType);
  }

  get selectedMethodUsesHttpOutbound(): boolean {
    return isHttpOutboundRpcBinding(this.selectedMethod?.bindingType)
      || isMqttCustomRpcBinding(this.selectedMethod?.bindingType);
  }

  get editableInvokeKeys(): string[] {
    const fixed = new Set(this.methodFixedEntries.map(e => e.platformKey));
    return this.methodPlatformKeys.filter(k => !fixed.has(k));
  }

  allFieldRows: DeviceRpcInvokeFieldRow[] = [];

  fieldRows: DeviceRpcInvokeFieldRow[] = [];

  fixedParamSummary: string[] = [];

  deviceRpcParamDefaults: Record<string, unknown> = {};

  rpcParamDefaultsByMethod: Record<string, Record<string, unknown>> = {};

  private deviceDataForRpcDefaults: {
    rpcParamDefaultsByMethod?: Record<string, Record<string, unknown>>;
    rpcParamDefaults?: Record<string, unknown>;
  } = {};

  methodPlatformKeys: string[] = [];

  methodFieldMeta: Record<string, DeviceRpcMethodFieldMeta> = {};

  methodFixedEntries: DeviceRpcFixedParamEntry[] = [];

  newFixedKey = '';

  newFixedValue = '';

  savingFixedParams = false;



  usesFieldInputs = false;

  valuesJson = '{}';

  nativeParamsJson = '{}';

  lastBuiltHex: string | null = null;



  sending = false;

  lastResponse: unknown = null;

  lastError: string | null = null;



  constructor(private store: Store<AppState>,

              private fb: UntypedFormBuilder,

              private deviceService: DeviceService,

              private deviceProfileService: DeviceProfileService,

              private bundleService: ProtocolTemplateBundleService,

              private translate: TranslateService,

              private destroyRef: DestroyRef,

              private cd: ChangeDetectorRef) {

    this.invokeForm = this.fb.group({

      methodId: ['', Validators.required]

    });

    this.invokeForm.get('methodId').valueChanges.pipe(takeUntilDestroyed(this.destroyRef))

      .subscribe((id: string) => this.onMethodSelected(id));

  }



  ngOnChanges(changes: SimpleChanges): void {

    if ((changes.active || changes.device) && this.active && this.device?.id) {

      this.reloadCatalog();

    }

  }



  get hasCatalog(): boolean {

    return this.catalogMethods.length > 0;

  }



  get availableKeysToAdd(): string[] {

    const fixed = new Set(this.methodFixedEntries.map(e => e.platformKey));

    return this.methodPlatformKeys.filter(k => !fixed.has(k));

  }

  templateFieldLabel(platformKey: string): string {
    return templateFieldKeyForPlatformKey(platformKey, this.selectedMethod?.paramMap);
  }

  hasPlatformAlias(platformKey: string): boolean {
    return this.templateFieldLabel(platformKey) !== platformKey;
  }

  getInvokeParamValue(platformKey: string): string {
    const parsed = parseNativeParamsJson(this.nativeParamsJson) ?? {};
    const v = parsed[platformKey];
    if (v === null || v === undefined) {
      return '';
    }
    return typeof v === 'object' ? JSON.stringify(v) : String(v);
  }

  setInvokeParamValue(platformKey: string, valueText: string): void {
    const parsed = parseNativeParamsJson(this.nativeParamsJson) ?? {};
    const trimmed = (valueText ?? '').trim();
    if (trimmed === '') {
      delete parsed[platformKey];
    } else if (/^-?\d+$/.test(trimmed)) {
      parsed[platformKey] = Number(trimmed);
    } else if (/^-?\d+\.\d+$/.test(trimmed)) {
      parsed[platformKey] = parseFloat(trimmed);
    } else if (trimmed === 'true' || trimmed === 'false') {
      parsed[platformKey] = trimmed === 'true';
    } else {
      parsed[platformKey] = trimmed;
    }
    this.nativeParamsJson = Object.keys(parsed).length ? JSON.stringify(parsed, null, 2) : '{}';
  }

  onMethodSelected(methodId: string): void {

    this.lastBuiltHex = null;

    this.lastResponse = null;

    this.lastError = null;

    this.selectedMethod = this.catalogMethods.find(m => m.id === methodId) ?? null;

    this.allFieldRows = [];

    this.fieldRows = [];

    this.fixedParamSummary = [];

    this.usesFieldInputs = false;

    this.valuesJson = '{}';

    this.nativeParamsJson = '{}';

    this.newFixedKey = '';

    this.newFixedValue = '';

    if (!this.selectedMethod) {

      this.methodPlatformKeys = [];

      this.methodFieldMeta = {};

      this.methodFixedEntries = [];

      return;

    }

    this.methodPlatformKeys = catalogMethodPlatformKeys(this.bundle, this.selectedMethod);

    this.methodFieldMeta = catalogMethodFieldMetaMap(this.bundle, this.selectedMethod);

    this.deviceRpcParamDefaults = resolveMethodRpcDefaults(this.deviceDataForRpcDefaults, methodId);

    this.methodFixedEntries = fixedEntriesFromDefaults(this.deviceRpcParamDefaults, this.methodFieldMeta);

    this.applyInvokeFieldsForMethod();

  }



  addFixedParamEntry(): void {

    const key = (this.newFixedKey ?? '').trim();

    if (!key || this.methodFixedEntries.some(e => e.platformKey === key)) {

      return;

    }

    this.methodFixedEntries = [...this.methodFixedEntries, {

      platformKey: key,

      valueText: (this.newFixedValue ?? '').trim()

    }];

    this.newFixedKey = '';

    this.newFixedValue = '';

  }



  removeFixedParamEntry(key: string): void {

    this.methodFixedEntries = this.methodFixedEntries.filter(e => e.platformKey !== key);

  }



  /** 保存前将「选择字段 + 固定值」未点添加的行一并写入列表 */
  private flushPendingFixedParamEntry(): void {
    const key = (this.newFixedKey ?? '').trim();
    const val = (this.newFixedValue ?? '').trim();
    if (!key) {
      return;
    }
    const existing = this.methodFixedEntries.find(e => e.platformKey === key);
    if (existing) {
      existing.valueText = val;
    } else if (val) {
      this.methodFixedEntries = [...this.methodFixedEntries, { platformKey: key, valueText: val }];
    }
    this.newFixedKey = '';
    this.newFixedValue = '';
  }

  saveFixedParams(): void {

    const deviceId = this.device?.id?.id;

    const methodId = this.selectedMethod?.id;

    if (!deviceId || !methodId || this.savingFixedParams) {

      return;

    }

    this.flushPendingFixedParamEntry();

    const parsed = defaultsFromFixedEntries(this.methodFixedEntries, this.methodFieldMeta);

    if (parsed === null) {

      this.notifyError('device.rpc.fixed-param-parse-error');

      return;

    }

    this.savingFixedParams = true;

    this.cd.markForCheck();

    const byMethod = { ...this.rpcParamDefaultsByMethod, [methodId]: parsed };

    this.deviceService.getDevice(deviceId).pipe(

      switchMap(dev => {

        const existing = dev.deviceData ?? {} as Device['deviceData'];

        const updated: Device = {

          ...dev,

          deviceData: {

            ...existing,

            configuration: existing.configuration,

            transportConfiguration: existing.transportConfiguration,

            rpcParamDefaultsByMethod: byMethod

          }

        };

        return this.deviceService.saveDevice(updated).pipe(
          switchMap(() => this.deviceService.getDevice(deviceId))
        );

      }),

      finalize(() => {

        this.savingFixedParams = false;

        this.cd.markForCheck();

      }),

      takeUntilDestroyed(this.destroyRef)

    ).subscribe({

      next: (saved) => {
        const fromServer = rpcParamDefaultsByMethodFromDeviceData(saved.deviceData);
        this.rpcParamDefaultsByMethod = Object.keys(fromServer).length
          ? fromServer
          : { ...this.rpcParamDefaultsByMethod, ...byMethod };
        this.deviceDataForRpcDefaults = {
          rpcParamDefaultsByMethod: this.rpcParamDefaultsByMethod,
          rpcParamDefaults: saved.deviceData?.rpcParamDefaults
        };

        this.onMethodSelected(methodId);

        this.notifySuccess('device.rpc.save-fixed-success');

      },

      error: (err) => {

        const detail = err?.error?.message || err?.message || String(err);

        this.notifyError('device.rpc.save-fixed-failed', { detail });

      }

    });

  }



  sendRpc(): void {

    if (!this.device?.id || !this.selectedMethod || this.sending) {

      return;

    }

    const deviceId = this.device.id.id;

    const timeout = this.selectedMethod.timeoutMs && this.selectedMethod.timeoutMs > 0

      ? this.selectedMethod.timeoutMs

      : 10000;

    const oneWay = this.selectedMethod.oneWay !== false;



    if (isProtocolTemplateRpcBinding(this.selectedMethod.bindingType)) {

      this.sendTcpTemplate(deviceId, oneWay, timeout);

    } else if (isHttpOutboundRpcBinding(this.selectedMethod.bindingType)
      || isMqttCustomRpcBinding(this.selectedMethod.bindingType)) {

      this.sendHttpOutbound(deviceId, oneWay, timeout);

    } else {

      this.sendNative(deviceId, oneWay, timeout);

    }

  }



  private applyInvokeFieldsForMethod(): void {

    const m = this.selectedMethod!;

    if (isProtocolTemplateRpcBinding(m.bindingType) && this.bundle) {

      const built = buildInvokeFieldRows(this.bundle, m);

      const applied = applyDeviceRpcParamDefaults(built.rows, this.deviceRpcParamDefaults, this.methodFieldMeta);

      this.allFieldRows = applied.allRows;

      this.fieldRows = applied.editableRows;

      this.fixedParamSummary = applied.fixedSummary;

      this.usesFieldInputs = built.usesFieldInputs;

      if (this.usesFieldInputs) {

        this.valuesJson = '{}';

      } else {

        const skeleton = parseNativeParamsJson(built.valuesJsonFallback) ?? {};

        const editableOnly: Record<string, unknown> = {};

        for (const [k, v] of Object.entries(skeleton)) {

          if (!Object.prototype.hasOwnProperty.call(this.deviceRpcParamDefaults, k)) {

            editableOnly[k] = v;

          }

        }

        this.valuesJson = Object.keys(editableOnly).length ? JSON.stringify(editableOnly, null, 2) : '{}';

      }

    } else if (isHttpOutboundRpcBinding(m.bindingType) || isMqttCustomRpcBinding(m.bindingType)) {

      const keys = catalogMethodPlatformKeys(null, m);

      const editableOnly: Record<string, unknown> = {};

      for (const k of keys) {

        if (!Object.prototype.hasOwnProperty.call(this.deviceRpcParamDefaults, k)) {

          editableOnly[k] = '';

        }

      }

      this.nativeParamsJson = Object.keys(editableOnly).length ? JSON.stringify(editableOnly, null, 2) : '{}';

      this.fixedParamSummary = Object.entries(this.deviceRpcParamDefaults)

        .map(([k, v]) => `${k}=${v}`);

    } else {

      const profileDefaults = parseNativeParamsJson(m.paramsTemplateJson ?? '') ?? {};

      const editableOnly: Record<string, unknown> = {};

      for (const [k, v] of Object.entries(profileDefaults)) {

        if (!Object.prototype.hasOwnProperty.call(this.deviceRpcParamDefaults, k)) {

          editableOnly[k] = v;

        }

      }

      this.nativeParamsJson = Object.keys(editableOnly).length ? JSON.stringify(editableOnly, null, 2) : '{}';

      this.fixedParamSummary = Object.entries(this.deviceRpcParamDefaults)

        .map(([k, v]) => `${k}=${v}`);

    }

  }



  private sendHttpOutbound(deviceId: string, oneWay: boolean, timeout: number): void {

    const m = this.selectedMethod!;

    const userParams = parseNativeParamsJson(this.nativeParamsJson);

    if (userParams === null) {

      this.notifyError('device.rpc.params-invalid');

      return;

    }

    const missing = this.editableInvokeKeys.filter(k => {
      const v = userParams[k];
      return v === null || v === undefined || v === '';
    });
    if (missing.length) {
      this.notifyError('device.rpc.missing-invoke-params', { fields: missing.join(', ') });
      return;
    }

    const mergedUser = mergeRpcPlatformValues(this.deviceRpcParamDefaults, userParams);

    const requestBody = {

      method: m.id,

      params: mergedUser,

      timeout

    };

    this.sending = true;

    this.lastError = null;

    this.lastResponse = null;

    this.cd.markForCheck();

    const call = oneWay

      ? this.deviceService.sendOneWayRpcCommand(deviceId, requestBody)

      : this.deviceService.sendTwoWayRpcCommand(deviceId, requestBody);

    call.pipe(

      defaultIfEmpty(null),

      finalize(() => {

        this.sending = false;

        this.cd.markForCheck();

      }),

      takeUntilDestroyed(this.destroyRef)

    ).subscribe({

      next: (res) => {

        this.lastResponse = res;

        this.notifySuccess('device.rpc.send-success');

      },

      error: (err) => {

        this.lastError = err?.error?.message || err?.message || String(err);

        this.notifyError('device.rpc.send-failed', { detail: this.lastError });

      }

    });

  }



  private sendTcpTemplate(deviceId: string, oneWay: boolean, timeout: number): void {

    const m = this.selectedMethod!;

    if (!this.bundleId || !this.bundle) {

      this.notifyError('device.rpc.bundle-missing');

      return;

    }

    const cmd = findBundleCommand(this.bundle, m);

    if (!cmd) {

      this.notifyError('device.rpc.command-not-found');

      return;

    }

    let platformValues: Record<string, unknown>;

    if (this.usesFieldInputs) {

      const editableKeys = new Set(this.fieldRows.map(r => r.platformKey));

      platformValues = {};

      for (const row of this.allFieldRows) {

        const parsed = parseDownlinkFieldInput(row.inputText, row.valueType, row.byteLength);

        if (parsed.ok !== true) {

          this.notifyError('profiles.protocol-hex-test-field-parse-error', { key: row.platformKey, detail: parsed.error });

          return;

        }

        platformValues[row.platformKey] = parsed.value;

        if (editableKeys.has(row.platformKey)) {

          row.inputText = formatDownlinkFieldEcho(parsed.value, row.valueType, parsed.echoMode, row.byteLength);

        }

      }

    } else {

      const parsed = parseNativeParamsJson(this.valuesJson);

      if (parsed === null) {

        this.notifyError('profiles.protocol-hex-test-values-json-invalid');

        return;

      }

      platformValues = mergeRpcPlatformValues(this.deviceRpcParamDefaults, parsed);

    }

    const values = applyParamMapToTemplateValues(platformValues, m.paramMap);

    const matchVt = cmd.matchValueType;

    const buildBody: Parameters<ProtocolTemplateBundleService['buildHex']>[0] = {

      bundleId: this.bundleId,

      templateId: cmd.templateId,

      values

    };

    if (isTcpHexVariableByteSlice(matchVt)) {

      buildBody.commandMatchBytesHex = cmd.commandMatchBytesHex ?? '';

    } else {

      buildBody.commandValue = cmd.commandValue;

    }



    this.sending = true;

    this.lastError = null;

    this.lastResponse = null;

    this.cd.markForCheck();

    this.bundleService.buildHex(buildBody).pipe(

      switchMap(buildResult => {

        if (!buildResult?.success || !buildResult.hex) {

          throw new Error(buildResult?.errorMessage || this.translate.instant('device.rpc.build-failed'));

        }

        this.lastBuiltHex = buildResult.hex;

        const requestBody = {

          method: m.id,

          params: { hex: buildResult.hex },

          timeout

        };

        const rpc$ = oneWay

          ? this.deviceService.sendOneWayRpcCommand(deviceId, requestBody)

          : this.deviceService.sendTwoWayRpcCommand(deviceId, requestBody);

        return rpc$.pipe(defaultIfEmpty(null));

      }),

      finalize(() => {

        this.sending = false;

        this.cd.markForCheck();

      }),

      takeUntilDestroyed(this.destroyRef)

    ).subscribe({

      next: (res) => {

        this.lastResponse = res;

        this.notifySuccess('device.rpc.send-success');

      },

      error: (err) => {

        this.lastError = err?.error?.message || err?.message || String(err);

        this.notifyError('device.rpc.send-failed', { detail: this.lastError });

      }

    });

  }



  private sendNative(deviceId: string, oneWay: boolean, timeout: number): void {

    const m = this.selectedMethod!;

    const userParams = parseNativeParamsJson(this.nativeParamsJson);

    if (userParams === null) {

      this.notifyError('device.rpc.params-invalid');

      return;

    }

    const mergedUser = mergeRpcPlatformValues(this.deviceRpcParamDefaults, userParams);

    const params = mergeNativeParams(m.paramsTemplateJson, mergedUser);

    const requestBody = {

      method: m.deviceMethod,

      params,

      timeout

    };

    this.sending = true;

    this.lastError = null;

    this.lastResponse = null;

    this.cd.markForCheck();

    const call = oneWay

      ? this.deviceService.sendOneWayRpcCommand(deviceId, requestBody)

      : this.deviceService.sendTwoWayRpcCommand(deviceId, requestBody);

    call.pipe(

      defaultIfEmpty(null),

      finalize(() => {

        this.sending = false;

        this.cd.markForCheck();

      }),

      takeUntilDestroyed(this.destroyRef)

    ).subscribe({

      next: (res) => {

        this.lastResponse = res;

        this.notifySuccess('device.rpc.send-success');

      },

      error: (err) => {

        this.lastError = err?.error?.message || err?.message || String(err);

        this.notifyError('device.rpc.send-failed', { detail: this.lastError });

      }

    });

  }



  methodOptionLabel(m: DeviceProfileRpcMethod): string {

    return catalogMethodLabel(m);

  }



  private reloadCatalog(): void {

    const deviceId = this.device?.id?.id;

    if (!deviceId) {

      return;

    }

    this.loading = true;

    this.catalogMethods = [];

    this.bundle = null;

    this.bundleId = null;

    this.deviceService.getDevice(deviceId).pipe(

      switchMap(dev => {

        this.rpcParamDefaultsByMethod = rpcParamDefaultsByMethodFromDeviceData(dev.deviceData);

        this.deviceDataForRpcDefaults = {
          rpcParamDefaultsByMethod: dev.deviceData?.rpcParamDefaultsByMethod,
          rpcParamDefaults: dev.deviceData?.rpcParamDefaults
        };

        const profileId = dev.deviceProfileId?.id;

        if (!profileId) {

          return of({ profile: null, bundle: null as ProtocolTemplateBundle | null });

        }

        return this.loadProfileAndBundle(profileId);

      }),

      takeUntilDestroyed(this.destroyRef)

    ).subscribe({

      next: ({ profile, bundle }) => {

        this.loading = false;

        if (!profile) {

          return;

        }

        this.transportType = profile.transportType;

        this.catalogMethods = (profile.profileData?.rpcMethods ?? []).filter(m => m?.id);

        this.bundle = bundle;

        this.bundleId = bundle?.id ?? null;

        if (this.catalogMethods.length) {

          const firstId = this.catalogMethods[0].id;

          this.invokeForm.patchValue({ methodId: firstId }, { emitEvent: true });

        } else {

          this.invokeForm.patchValue({ methodId: '' }, { emitEvent: false });

          this.selectedMethod = null;

        }

      },

      error: () => {

        this.loading = false;

        this.notifyError('device.rpc.load-failed');

      }

    });

  }



  private notifySuccess(key: string): void {

    this.store.dispatch(new ActionNotificationShow({

      message: this.translate.instant(key),

      type: 'success',

      duration: 3000,

      verticalPosition: 'top',

      horizontalPosition: 'right'

    }));

  }



  private loadProfileAndBundle(profileId: string) {

    return this.deviceProfileService.getDeviceProfile(profileId).pipe(

      switchMap(profile => {

        if (isProtocolTemplateWireTransport(profile.transportType) && profile.profileData?.transportConfiguration) {

          const bid = protocolTemplateBundleIdFromWireProfile(
            profile.profileData.transportConfiguration as TcpDeviceProfileTransportConfiguration | UdpDeviceProfileTransportConfiguration);

          if (bid) {

            return this.bundleService.getBundles().pipe(

              map(bundles => ({ profile, bundle: bundles.find(b => b.id === bid) ?? null }))

            );

          }

        }

        return of({ profile, bundle: null as ProtocolTemplateBundle | null });

      })

    );

  }



  private notifyError(key: string, interpolateParams?: object): void {

    this.store.dispatch(new ActionNotificationShow({

      message: this.translate.instant(key, interpolateParams),

      type: 'error',

      duration: 5000,

      verticalPosition: 'top',

      horizontalPosition: 'right'

    }));

  }

}

