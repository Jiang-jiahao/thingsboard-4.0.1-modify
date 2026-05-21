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
  TcpDeviceProfileTransportConfiguration
} from '@shared/models/device.models';
import { EntityId } from '@shared/models/id/entity-id';
import {
  applyParamMapToTemplateValues,
  buildInvokeFieldRows,
  catalogMethodLabel,
  DeviceRpcInvokeFieldRow,
  findBundleCommand,
  mergeNativeParams,
  parseNativeParamsJson,
  protocolTemplateBundleIdFromTcpProfile
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
  fieldRows: DeviceRpcInvokeFieldRow[] = [];
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

  onMethodSelected(methodId: string): void {
    this.lastBuiltHex = null;
    this.lastResponse = null;
    this.lastError = null;
    this.selectedMethod = this.catalogMethods.find(m => m.id === methodId) ?? null;
    this.fieldRows = [];
    this.usesFieldInputs = false;
    this.valuesJson = '{}';
    this.nativeParamsJson = '{}';
    if (!this.selectedMethod) {
      return;
    }
    if (this.selectedMethod.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE && this.bundle) {
      const built = buildInvokeFieldRows(this.bundle, this.selectedMethod);
      this.fieldRows = built.rows;
      this.usesFieldInputs = built.usesFieldInputs;
      this.valuesJson = built.valuesJsonFallback;
    } else if (this.selectedMethod.paramsTemplateJson?.trim()) {
      this.nativeParamsJson = this.selectedMethod.paramsTemplateJson.trim();
    }
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

    if (this.selectedMethod.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE) {
      this.sendTcpTemplate(deviceId, oneWay, timeout);
    } else {
      this.sendNative(deviceId, oneWay, timeout);
    }
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
      platformValues = {};
      for (const row of this.fieldRows) {
        const parsed = parseDownlinkFieldInput(row.inputText, row.valueType, row.byteLength);
        if (parsed.ok !== true) {
          this.notifyError('profiles.protocol-hex-test-field-parse-error', { key: row.platformKey, detail: parsed.error });
          return;
        }
        platformValues[row.platformKey] = parsed.value;
        row.inputText = formatDownlinkFieldEcho(parsed.value, row.valueType, parsed.echoMode, row.byteLength);
      }
    } else {
      const parsed = parseNativeParamsJson(this.valuesJson);
      if (parsed === null) {
        this.notifyError('profiles.protocol-hex-test-values-json-invalid');
        return;
      }
      platformValues = parsed;
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
    const params = mergeNativeParams(m.paramsTemplateJson, userParams);
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
        if (profile.transportType === DeviceTransportType.TCP && profile.profileData?.transportConfiguration) {
          const tcp = profile.profileData.transportConfiguration as TcpDeviceProfileTransportConfiguration;
          const bid = protocolTemplateBundleIdFromTcpProfile(tcp);
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
