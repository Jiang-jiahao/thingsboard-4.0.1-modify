///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { selectAuthUser, selectIsUserLoaded } from '@core/auth/auth.selectors';
import { ProtocolTemplateBundleService } from '@core/services/protocol-template-bundle.service';
import { NULL_UUID } from '@shared/models/id/has-uuid';
import {
  ProtocolTemplateBundle
} from '@shared/models/device.models';
import { combineLatest } from 'rxjs';
import { filter, switchMap, take } from 'rxjs/operators';
import { deepClone } from '@core/utils';
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
  DeviceTransportType,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateDefinition,
  TcpHexChecksumDefinition,
  TcpDeviceProfileTransportConfiguration,
  TCP_HEX_MATCH_VALUE_TYPES,
  TcpHexCommandProfile,
  TcpHexFieldDefinition,
  TcpHexLtvChunkOrder,
  TcpHexLtvRepeatingConfig,
  TcpHexLtvTagMapping,
  TcpHexUnknownTagMode,
  TcpHexValueType,
  TcpJsonWithoutMethodMode,
  TcpTransportConnectMode,
  TcpTransportFramingMode,
  TcpWireAuthenticationMode,
  TransportTcpDataType,
  normalizeTransportTcpDataType
} from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
@Component({
  selector: 'tb-tcp-device-profile-transport-configuration',
  templateUrl: './tcp-device-profile-transport-configuration.component.html',
  styleUrls: ['./tcp-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => TcpDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => TcpDeviceProfileTransportConfigurationComponent),
      multi: true
    }]
})
export class TcpDeviceProfileTransportConfigurationComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {
  tcpTransportFramingModes = Object.values(TcpTransportFramingMode);
  /** 显式列出，避免部分环境下 Object.values(enum) 与下拉展示不一致 */
  transportTcpDataTypes: TransportTcpDataType[] = [
    TransportTcpDataType.JSON,
    TransportTcpDataType.HEX,
    TransportTcpDataType.ASCII
  ];
  tcpHexValueTypes = Object.values(TcpHexValueType);
  tcpHexMatchValueTypes = TCP_HEX_MATCH_VALUE_TYPES;
  readonly TransportTcpDataType = TransportTcpDataType;
  readonly TcpHexValueType = TcpHexValueType;
  readonly TcpHexLtvChunkOrder = TcpHexLtvChunkOrder;
  readonly TcpHexUnknownTagMode = TcpHexUnknownTagMode;
  tcpDeviceProfileTransportConfigurationFormGroup: UntypedFormGroup;
  private destroy$ = new Subject<void>();
  @Input()
  disabled: boolean;

  /** 租户「协议模板」库，用于协议模板负载时下拉选择 */
  protocolTemplateBundles: ProtocolTemplateBundle[] = [];

  private propagateChange = (v: any) => {
  };
  constructor(
    private fb: UntypedFormBuilder,
    private store: Store<AppState>,
    private protocolTemplateBundleService: ProtocolTemplateBundleService
  ) {
  }
  ngOnInit(): void {
    combineLatest([
      this.store.select(selectIsUserLoaded),
      this.store.select(selectAuthUser)
    ]).pipe(
      filter(([loaded, u]) => loaded && !!u?.tenantId && u.tenantId !== NULL_UUID),
      take(1),
      switchMap(() => this.protocolTemplateBundleService.getBundles()),
      take(1)
    ).subscribe(bundles => {
      this.protocolTemplateBundles = bundles ?? [];
    });

    this.tcpDeviceProfileTransportConfigurationFormGroup = this.fb.group({
      tcpTransportConnectMode: [TcpTransportConnectMode.SERVER, Validators.required],
      tcpTransportFramingMode: [TcpTransportFramingMode.LINE, Validators.required],
      tcpFixedFrameLength: [null],
      tcpWireAuthenticationMode: [TcpWireAuthenticationMode.TOKEN, Validators.required],
      tcpOutboundReconnectIntervalSec: [null, [Validators.min(0)]],
      tcpOutboundReconnectMaxAttempts: [null, [Validators.min(0)]],
      tcpReadIdleTimeoutSec: [null, [Validators.min(0)]],
      tcpJsonWithoutMethodMode: [TcpJsonWithoutMethodMode.TELEMETRY_FLAT, Validators.required],
      tcpOpaqueRuleEngineKey: ['tcpOpaquePayload'],
      dataType: [TransportTcpDataType.JSON, Validators.required],
      hexCommandProfiles: this.fb.array([]),
      hexProtocolFields: this.fb.array([]),
      hexLtvEnabled: [false],
      hexLtvStartOffset: [0, [Validators.min(0)]],
      hexLtvLengthType: [TcpHexValueType.UINT8, Validators.required],
      hexLtvTagType: [TcpHexValueType.UINT8, Validators.required],
      hexLtvChunkOrder: [TcpHexLtvChunkOrder.LTV, Validators.required],
      hexLtvMaxItems: [32, [Validators.min(1)]],
      hexLtvKeyPrefix: ['ltv'],
      hexLtvUnknownMode: [TcpHexUnknownTagMode.SKIP, Validators.required],
      hexLtvTagMappings: this.fb.array([]),
      protocolTemplateBundleId: [null],
      protocolTemplates: this.fb.array([]),
      protocolCommands: this.fb.array([]),
      hexChecksumType: ['NONE'],
      hexChecksumFromByte: [0],
      hexChecksumToExclusive: [0],
      hexChecksumByteIndex: [-2]
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpTransportFramingMode').valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe((mode: TcpTransportFramingMode) => {
      const fixedCtrl = this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpFixedFrameLength');
      if (mode === TcpTransportFramingMode.FIXED_LENGTH) {
        fixedCtrl.setValidators([Validators.required, Validators.min(1), Validators.pattern('[0-9]*')]);
      } else {
        fixedCtrl.clearValidators();
        fixedCtrl.patchValue(null, {emitEvent: false});
      }
      fixedCtrl.updateValueAndValidity({emitEvent: false});
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe(() => {
      this.updateModel();
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.get('dataType').valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe((dt: TransportTcpDataType) => {
      if (dt !== TransportTcpDataType.HEX) {
        this.patchProtocolTemplatesFromModel([]);
        this.patchProtocolCommandsFromModel([]);
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue(
          { protocolTemplateBundleId: null },
          { emitEvent: false }
        );
        this.patchHexCommandProfilesFromModel([]);
        this.patchHexFieldsFromModel([]);
        this.patchHexLtvDisabled();
      }
    });
  }

  /** 在负载类型为 HEX 时，是否按「协议模板包」保存（序列化为 PROTOCOL_TEMPLATE） */
  private usesProtocolTemplatePayload(v: Record<string, unknown>): boolean {
    if (v['dataType'] !== TransportTcpDataType.HEX) {
      return false;
    }
    if (this.hasManualHexConfiguration()) {
      return false;
    }
    const bundleId = v['protocolTemplateBundleId'];
    if (bundleId != null && String(bundleId).trim() !== '') {
      return true;
    }
    return (this.protocolCommandsArray?.length ?? 0) > 0;
  }

  /**
   * 已选协议模板包时不显示下方手动解析（规则来自模板库）；未选模板包时仅当已有手动 HEX 数据（如遗留配置）时显示以便编辑。
   */
  get showHexManualParsingSection(): boolean {
    const bundleId = this.tcpDeviceProfileTransportConfigurationFormGroup?.get('protocolTemplateBundleId')?.value;
    if (bundleId != null && String(bundleId).trim() !== '') {
      return false;
    }
    return this.hasManualHexConfiguration();
  }

  private hasManualHexConfiguration(): boolean {
    const cmdRows = (this.hexCommandProfilesArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
    for (const row of cmdRows) {
      const fields = (row['fields'] as Array<Record<string, unknown>>) ?? [];
      if (fields.some(f => f && String(f['key'] ?? '').trim())) {
        return true;
      }
    }
    const defaultRows = (this.hexProtocolFieldsArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
    if (defaultRows.some(r => r && String(r['key'] ?? '').trim())) {
      return true;
    }
    if (this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvEnabled')?.value) {
      return true;
    }
    return false;
  }

  get hexProtocolFieldsArray(): UntypedFormArray {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexProtocolFields') as UntypedFormArray;
  }

  get hexCommandProfilesArray(): UntypedFormArray {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexCommandProfiles') as UntypedFormArray;
  }

  get hexLtvTagMappingsArray(): UntypedFormArray {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvTagMappings') as UntypedFormArray;
  }

  get protocolTemplatesArray(): UntypedFormArray {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.get('protocolTemplates') as UntypedFormArray;
  }

  get protocolCommandsArray(): UntypedFormArray {
    return this.tcpDeviceProfileTransportConfigurationFormGroup.get('protocolCommands') as UntypedFormArray;
  }

  getCommandFieldsArray(commandIndex: number): UntypedFormArray {
    return (this.hexCommandProfilesArray.at(commandIndex) as UntypedFormGroup).get('fields') as UntypedFormArray;
  }

  private createCommandProfileGroup(p?: TcpHexCommandProfile): UntypedFormGroup {
    const fieldsArr = this.fb.array([] as UntypedFormGroup[]);
    if (p?.fields?.length) {
      for (const f of p.fields) {
        fieldsArr.push(this.createHexFieldGroup(f));
      }
    }
    return this.fb.group({
      name: [p?.name ?? ''],
      matchByteOffset: [p?.matchByteOffset ?? 0, [Validators.required, Validators.min(0)]],
      matchValueType: [p?.matchValueType ?? TcpHexValueType.UINT8, Validators.required],
      matchValue: [p?.matchValue ?? 0, Validators.required],
      secondaryMatchByteOffset: [p?.secondaryMatchByteOffset ?? null],
      secondaryMatchValueType: [p?.secondaryMatchValueType ?? TcpHexValueType.UINT8],
      secondaryMatchValue: [p?.secondaryMatchValue ?? null],
      fields: fieldsArr
    });
  }

  private patchHexCommandProfilesFromModel(profiles: TcpHexCommandProfile[]) {
    const arr = this.hexCommandProfilesArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    if (profiles?.length) {
      for (const p of profiles) {
        arr.push(this.createCommandProfileGroup(p), {emitEvent: false});
      }
    }
  }

  addHexCommandProfile() {
    this.hexCommandProfilesArray.push(this.createCommandProfileGroup());
    this.updateModel();
  }

  removeHexCommandProfile(index: number) {
    this.hexCommandProfilesArray.removeAt(index);
    this.updateModel();
  }

  addFieldToCommand(commandIndex: number) {
    this.getCommandFieldsArray(commandIndex).push(this.createHexFieldGroup());
    this.updateModel();
  }

  removeFieldFromCommand(commandIndex: number, fieldIndex: number) {
    this.getCommandFieldsArray(commandIndex).removeAt(fieldIndex);
    this.updateModel();
  }

  private createLtvTagMappingGroup(m?: TcpHexLtvTagMapping): UntypedFormGroup {
    return this.fb.group({
      tagValue: [m?.tagValue ?? 0, Validators.required],
      telemetryKey: [m?.telemetryKey ?? '', [Validators.maxLength(255)]],
      valueType: [m?.valueType ?? TcpHexValueType.UINT8, Validators.required],
      byteLength: [m?.byteLength ?? null, [Validators.min(1)]]
    });
  }

  private patchHexLtvFromModel(cfg: TcpHexLtvRepeatingConfig | undefined) {
    const arr = this.hexLtvTagMappingsArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    if (cfg?.tagMappings?.length) {
      for (const m of cfg.tagMappings) {
        arr.push(this.createLtvTagMappingGroup(m), {emitEvent: false});
      }
    }
    this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
      hexLtvEnabled: !!cfg,
      hexLtvStartOffset: cfg?.startByteOffset ?? 0,
      hexLtvLengthType: cfg?.lengthFieldType ?? TcpHexValueType.UINT8,
      hexLtvTagType: cfg?.tagFieldType ?? TcpHexValueType.UINT8,
      hexLtvChunkOrder: cfg?.chunkOrder ?? TcpHexLtvChunkOrder.LTV,
      hexLtvMaxItems: cfg?.maxItems ?? 32,
      hexLtvKeyPrefix: cfg?.keyPrefix ?? 'ltv',
      hexLtvUnknownMode: cfg?.unknownTagMode ?? TcpHexUnknownTagMode.SKIP
    }, {emitEvent: false});
  }

  private patchHexLtvDisabled() {
    this.patchHexLtvFromModel(undefined);
    this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({hexLtvEnabled: false}, {emitEvent: false});
  }

  addHexLtvTagMappingRow() {
    this.hexLtvTagMappingsArray.push(this.createLtvTagMappingGroup());
    this.updateModel();
  }

  removeHexLtvTagMappingRow(index: number) {
    this.hexLtvTagMappingsArray.removeAt(index);
    this.updateModel();
  }

  isBytesAsHexLtvTagRow(index: number): boolean {
    const g = this.hexLtvTagMappingsArray.at(index) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  isBytesAsHexCommandRow(commandIndex: number, fieldIndex: number): boolean {
    const g = this.getCommandFieldsArray(commandIndex).at(fieldIndex) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  private createHexFieldGroup(f?: TcpHexFieldDefinition): UntypedFormGroup {
    const fromFrame = f?.byteLengthFromByteOffset != null && f.byteLengthFromByteOffset >= 0;
    return this.fb.group({
      key: [f?.key ?? '', [Validators.maxLength(255)]],
      byteOffset: [f?.byteOffset ?? 0, [Validators.required, Validators.min(0)]],
      valueType: [f?.valueType ?? TcpHexValueType.UINT8, Validators.required],
      hexFieldLengthMode: [fromFrame ? 'fromFrame' : 'fixed'],
      byteLength: [f?.byteLength ?? null, [Validators.min(1)]],
      byteLengthFromByteOffset: [f?.byteLengthFromByteOffset ?? null, [Validators.min(0)]],
      byteLengthFromValueType: [f?.byteLengthFromValueType ?? TcpHexValueType.UINT8, Validators.required]
    });
  }

  private patchHexFieldsFromModel(fields: TcpHexFieldDefinition[]) {
    const arr = this.hexProtocolFieldsArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    if (fields?.length) {
      for (const f of fields) {
        arr.push(this.createHexFieldGroup(f), {emitEvent: false});
      }
    }
  }

  addHexProtocolField() {
    this.hexProtocolFieldsArray.push(this.createHexFieldGroup());
    this.updateModel();
  }

  removeHexProtocolField(index: number) {
    this.hexProtocolFieldsArray.removeAt(index);
    this.updateModel();
  }

  private createProtocolTemplateGroup(t?: ProtocolTemplateDefinition): UntypedFormGroup {
    const fieldsArr = this.fb.array([] as UntypedFormGroup[]);
    if (t?.hexProtocolFields?.length) {
      for (const f of t.hexProtocolFields) {
        fieldsArr.push(this.createHexFieldGroup(f));
      }
    }
    const ltvArr = this.fb.array([] as UntypedFormGroup[]);
    const ltv = t?.hexLtvRepeating;
    if (ltv?.tagMappings?.length) {
      for (const m of ltv.tagMappings) {
        ltvArr.push(this.createLtvTagMappingGroup(m));
      }
    }
    return this.fb.group({
      id: [t?.id ?? '', [Validators.required, Validators.maxLength(255)]],
      name: [t?.name ?? '', [Validators.maxLength(255)]],
      commandByteOffset: [t?.commandByteOffset ?? 12, [Validators.required, Validators.min(0)]],
      commandMatchWidth: [t?.commandMatchWidth === 1 ? 1 : 4],
      validateTotalLengthU32Le: [t?.validateTotalLengthU32Le === true],
      hexProtocolFields: fieldsArr,
      hexLtvEnabled: [!!ltv],
      hexLtvStartOffset: [ltv?.startByteOffset ?? 0, [Validators.required, Validators.min(0)]],
      hexLtvLengthType: [ltv?.lengthFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvTagType: [ltv?.tagFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvChunkOrder: [ltv?.chunkOrder ?? TcpHexLtvChunkOrder.LTV, Validators.required],
      hexLtvMaxItems: [ltv?.maxItems ?? 32, [Validators.min(1)]],
      hexLtvKeyPrefix: [ltv?.keyPrefix ?? 'ltv'],
      hexLtvUnknownMode: [ltv?.unknownTagMode ?? TcpHexUnknownTagMode.SKIP, Validators.required],
      hexLtvTagMappings: ltvArr
    });
  }

  private createProtocolCommandGroup(c?: ProtocolTemplateCommandDefinition): UntypedFormGroup {
    const overrideArr = this.fb.array([] as UntypedFormGroup[]);
    if (c?.fields?.length) {
      for (const f of c.fields) {
        overrideArr.push(this.createHexFieldGroup(f));
      }
    }
    return this.fb.group({
      templateId: [c?.templateId ?? '', Validators.required],
      name: [c?.name ?? '', [Validators.maxLength(255)]],
      commandValue: [c?.commandValue ?? 0, Validators.required],
      matchValueType: [c?.matchValueType ?? TcpHexValueType.UINT32_LE, Validators.required],
      secondaryMatchByteOffset: [c?.secondaryMatchByteOffset ?? null],
      secondaryMatchValueType: [c?.secondaryMatchValueType ?? TcpHexValueType.UINT8],
      secondaryMatchValue: [c?.secondaryMatchValue ?? null],
      direction: [c?.direction ?? ProtocolTemplateCommandDirection.UPLINK, Validators.required],
      overrideFields: overrideArr
    });
  }

  private patchProtocolTemplatesFromModel(templates: ProtocolTemplateDefinition[]) {
    const arr = this.protocolTemplatesArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    const t = templates?.length ? templates[0] : undefined;
    arr.push(this.createProtocolTemplateGroup(t), {emitEvent: false});
  }

  private patchProtocolCommandsFromModel(commands: ProtocolTemplateCommandDefinition[]) {
    const arr = this.protocolCommandsArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    const defaultTid = this.protocolTemplatesArray?.length
      ? String((this.protocolTemplatesArray.at(0) as UntypedFormGroup).get('id')?.value ?? '').trim()
      : '';
    if (commands?.length) {
      for (const c of commands) {
        const merged: ProtocolTemplateCommandDefinition = {
          ...c,
          templateId: (c.templateId && String(c.templateId).trim()) ? c.templateId : defaultTid
        };
        arr.push(this.createProtocolCommandGroup(merged), {emitEvent: false});
      }
    }
  }

  addProtocolTemplate() {
    this.protocolTemplatesArray.push(this.createProtocolTemplateGroup());
    this.updateModel();
  }

  removeProtocolTemplate(index: number) {
    this.protocolTemplatesArray.removeAt(index);
    this.updateModel();
  }

  addFieldToProtocolTemplate(templateIndex: number) {
    this.getProtocolTemplateFieldsArray(templateIndex).push(this.createHexFieldGroup());
    this.updateModel();
  }

  removeFieldFromProtocolTemplate(payload: { templateIndex: number; fieldIndex: number }) {
    this.getProtocolTemplateFieldsArray(payload.templateIndex).removeAt(payload.fieldIndex);
    this.updateModel();
  }

  getProtocolTemplateFieldsArray(templateIndex: number): UntypedFormArray {
    return (this.protocolTemplatesArray.at(templateIndex) as UntypedFormGroup).get('hexProtocolFields') as UntypedFormArray;
  }

  addProtocolCommand() {
    this.protocolCommandsArray.push(this.createProtocolCommandGroup());
    this.updateModel();
  }

  removeProtocolCommand(index: number) {
    this.protocolCommandsArray.removeAt(index);
    this.updateModel();
  }

  addOverrideFieldToCommand(commandIndex: number) {
    this.getProtocolCommandOverrideFieldsArray(commandIndex).push(this.createHexFieldGroup());
    this.updateModel();
  }

  removeOverrideFieldFromCommand(payload: { commandIndex: number; fieldIndex: number }) {
    this.getProtocolCommandOverrideFieldsArray(payload.commandIndex).removeAt(payload.fieldIndex);
    this.updateModel();
  }

  addProtocolTemplateLtvMapping(templateIndex: number) {
    const tpl = this.protocolTemplatesArray.at(templateIndex) as UntypedFormGroup;
    (tpl.get('hexLtvTagMappings') as UntypedFormArray).push(this.createLtvTagMappingGroup());
    this.updateModel();
  }

  removeProtocolTemplateLtvMapping(payload: { templateIndex: number; index: number }) {
    const tpl = this.protocolTemplatesArray.at(payload.templateIndex) as UntypedFormGroup;
    (tpl.get('hexLtvTagMappings') as UntypedFormArray).removeAt(payload.index);
    this.updateModel();
  }

  getProtocolCommandOverrideFieldsArray(commandIndex: number): UntypedFormArray {
    return (this.protocolCommandsArray.at(commandIndex) as UntypedFormGroup).get('overrideFields') as UntypedFormArray;
  }

  get protocolTemplateBundleSelectedButMissing(): boolean {
    const id = this.tcpDeviceProfileTransportConfigurationFormGroup?.get('protocolTemplateBundleId')?.value;
    if (!id) {
      return false;
    }
    return !this.protocolTemplateBundles.some(b => b.id === id);
  }

  onProtocolTemplateBundleSelected(bundleId: string | null): void {
    if (!bundleId) {
      this.patchProtocolTemplatesFromModel([]);
      this.patchProtocolCommandsFromModel([]);
      this.updateModel();
      return;
    }
    const b = this.protocolTemplateBundles.find(x => x.id === bundleId);
    if (!b) {
      this.updateModel();
      return;
    }
    this.patchHexCommandProfilesFromModel([]);
    this.patchHexFieldsFromModel([]);
    this.patchHexLtvDisabled();
    this.patchProtocolTemplatesFromModel(deepClone(b.protocolTemplates));
    this.patchProtocolCommandsFromModel(deepClone(b.protocolCommands));
    this.updateModel();
  }

  isBytesAsHexRow(index: number): boolean {
    const g = this.hexProtocolFieldsArray.at(index) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  getHexFieldLengthModeDefault(i: number): 'fixed' | 'fromFrame' {
    return this.hexFieldLengthModeFromGroup(this.hexProtocolFieldsArray.at(i) as UntypedFormGroup);
  }

  onHexFieldLengthModeDefault(i: number, mode: 'fixed' | 'fromFrame'): void {
    this.applyHexFieldLengthMode(this.hexProtocolFieldsArray.at(i) as UntypedFormGroup, mode);
  }

  getHexFieldLengthModeCommand(ci: number, fi: number): 'fixed' | 'fromFrame' {
    return this.hexFieldLengthModeFromGroup(this.getCommandFieldsArray(ci).at(fi) as UntypedFormGroup);
  }

  onHexFieldLengthModeCommand(ci: number, fi: number, mode: 'fixed' | 'fromFrame'): void {
    this.applyHexFieldLengthMode(this.getCommandFieldsArray(ci).at(fi) as UntypedFormGroup, mode);
  }

  private hexFieldLengthModeFromGroup(g: UntypedFormGroup): 'fixed' | 'fromFrame' {
    const m = g.get('hexFieldLengthMode')?.value;
    if (m === 'fromFrame' || m === 'fixed') {
      return m;
    }
    const fromOff = g.get('byteLengthFromByteOffset')?.value;
    return fromOff !== null && fromOff !== undefined && fromOff !== '' ? 'fromFrame' : 'fixed';
  }

  private applyHexFieldLengthMode(g: UntypedFormGroup, mode: 'fixed' | 'fromFrame'): void {
    g.get('hexFieldLengthMode')?.patchValue(mode, { emitEvent: false });
    if (mode === 'fixed') {
      g.patchValue({ byteLengthFromByteOffset: null, byteLengthFromValueType: TcpHexValueType.UINT8 }, { emitEvent: false });
    } else {
      g.patchValue({ byteLength: null }, { emitEvent: false });
    }
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }
  registerOnTouched(fn: any): void {
  }
  setDisabledState(isDisabled: boolean) {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceProfileTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceProfileTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }
  writeValue(value: TcpDeviceProfileTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      const rawType = normalizeTransportTcpDataType(
        value.transportTcpDataTypeConfiguration?.transportTcpDataType
      ) || TransportTcpDataType.JSON;
      const formDataType = rawType === TransportTcpDataType.PROTOCOL_TEMPLATE
        ? TransportTcpDataType.HEX
        : rawType;
      this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
        tcpTransportConnectMode: value.tcpTransportConnectMode,
        tcpTransportFramingMode: value.tcpTransportFramingMode,
        tcpFixedFrameLength: value.tcpFixedFrameLength,
        tcpWireAuthenticationMode: value.tcpWireAuthenticationMode,
        tcpOutboundReconnectIntervalSec: value.tcpOutboundReconnectIntervalSec,
        tcpOutboundReconnectMaxAttempts: value.tcpOutboundReconnectMaxAttempts,
        tcpReadIdleTimeoutSec: value.tcpReadIdleTimeoutSec,
        tcpJsonWithoutMethodMode: value.tcpJsonWithoutMethodMode,
        tcpOpaqueRuleEngineKey: value.tcpOpaqueRuleEngineKey || 'tcpOpaquePayload',
        dataType: formDataType
      }, {emitEvent: false});
      const hcfg = value.transportTcpDataTypeConfiguration;
      if (rawType === TransportTcpDataType.PROTOCOL_TEMPLATE && hcfg) {
        this.patchProtocolTemplatesFromModel(
          hcfg.protocolTemplates ?? hcfg.monitoringTemplates
        );
        this.patchProtocolCommandsFromModel(
          hcfg.protocolCommands ?? hcfg.monitoringCommands
        );
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
          protocolTemplateBundleId: hcfg.protocolTemplateBundleId ?? hcfg.monitoringProtocolBundleId ?? null
        }, {emitEvent: false});
        this.patchHexCommandProfilesFromModel([]);
        this.patchHexFieldsFromModel([]);
        this.patchHexLtvDisabled();
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
          hexChecksumType: 'NONE',
          hexChecksumFromByte: 0,
          hexChecksumToExclusive: 0,
          hexChecksumByteIndex: -2
        }, {emitEvent: false});
      } else if (rawType === TransportTcpDataType.HEX && hcfg) {
        this.patchHexCommandProfilesFromModel(hcfg.hexCommandProfiles);
        if (hcfg.hexProtocolFields?.length) {
          this.patchHexFieldsFromModel(hcfg.hexProtocolFields);
        } else {
          this.patchHexFieldsFromModel([]);
        }
        if (hcfg.hexLtvRepeating) {
          this.patchHexLtvFromModel(hcfg.hexLtvRepeating);
        } else {
          this.patchHexLtvDisabled();
        }
        const cks = hcfg.checksum;
        const csType = cks?.type && String(cks.type).trim() && String(cks.type).toUpperCase() !== 'NONE'
          ? String(cks.type).trim()
          : 'NONE';
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
          hexChecksumType: csType,
          hexChecksumFromByte: cks?.fromByte ?? 0,
          hexChecksumToExclusive: cks?.toExclusive ?? 0,
          hexChecksumByteIndex: cks?.checksumByteIndex ?? -2
        }, {emitEvent: false});
        this.patchProtocolTemplatesFromModel([]);
        this.patchProtocolCommandsFromModel([]);
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
          protocolTemplateBundleId: null
        }, {emitEvent: false});
      } else {
        this.patchHexCommandProfilesFromModel([]);
        this.patchHexFieldsFromModel([]);
        this.patchHexLtvDisabled();
        this.patchProtocolTemplatesFromModel([]);
        this.patchProtocolCommandsFromModel([]);
        this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
          protocolTemplateBundleId: null
        }, {emitEvent: false});
      }
      const mode = value.tcpTransportFramingMode || TcpTransportFramingMode.LINE;
      const fixedCtrl = this.tcpDeviceProfileTransportConfigurationFormGroup.get('tcpFixedFrameLength');
      if (mode === TcpTransportFramingMode.FIXED_LENGTH) {
        fixedCtrl.setValidators([Validators.required, Validators.min(1), Validators.pattern('[0-9]*')]);
      } else {
        fixedCtrl.clearValidators();
      }
      fixedCtrl.updateValueAndValidity({emitEvent: false});
    }
  }
  private updateModel() {
    const v = this.tcpDeviceProfileTransportConfigurationFormGroup.getRawValue();
    const transportTcpDataTypeConfiguration: TcpDeviceProfileTransportConfiguration['transportTcpDataTypeConfiguration'] = {};

    if (v.dataType === TransportTcpDataType.HEX) {
      const usePt = this.usesProtocolTemplatePayload(v);
      if (usePt) {
        transportTcpDataTypeConfiguration.transportTcpDataType = TransportTcpDataType.PROTOCOL_TEMPLATE;
        const templates: ProtocolTemplateDefinition[] = [];
        const tplRows = (this.protocolTemplatesArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
        const row = tplRows[0];
        if (row) {
          const id = String(row['id'] ?? '').trim();
          if (id) {
            const fieldRows = (row['hexProtocolFields'] as Array<Record<string, unknown>>) ?? [];
            const fields = this.fieldsFromHexRows(fieldRows);
            const t: ProtocolTemplateDefinition = {
              id,
              commandByteOffset: Number(row['commandByteOffset']) ?? 12,
              commandMatchWidth: Number(row['commandMatchWidth']) === 1 ? 1 : 4,
              validateTotalLengthU32Le: row['validateTotalLengthU32Le'] === true
            };
            const csType = String(row['checksumType'] ?? 'NONE').trim();
            if (csType && csType.toUpperCase() !== 'NONE') {
              t.checksum = {
                type: csType,
                fromByte: Number(row['checksumFromByte']) || 0,
                toExclusive: Number(row['checksumToExclusive']) || 0,
                checksumByteIndex: Number(row['checksumByteIndex']) ?? -2
              };
            }
            const nm = String(row['name'] ?? '').trim();
            if (nm) {
              t.name = nm;
            }
            if (fields.length) {
              t.hexProtocolFields = fields;
            }
            if (row['hexLtvEnabled']) {
              const ltvTagRows = (row['hexLtvTagMappings'] as Array<Record<string, unknown>>) ?? [];
              const tagMappings = this.ltvTagMappingsFromRows(ltvTagRows);
              const ltv: TcpHexLtvRepeatingConfig = {
                startByteOffset: Number(row['hexLtvStartOffset']) || 0,
                lengthFieldType: row['hexLtvLengthType'] as TcpHexValueType,
                tagFieldType: row['hexLtvTagType'] as TcpHexValueType,
                chunkOrder: row['hexLtvChunkOrder'] as TcpHexLtvChunkOrder,
                keyPrefix: String(row['hexLtvKeyPrefix'] || 'ltv').trim() || 'ltv',
                unknownTagMode: row['hexLtvUnknownMode'] as TcpHexUnknownTagMode
              };
              const maxItems = this.optionalFormNumber(row['hexLtvMaxItems']);
              if (maxItems !== undefined) {
                ltv.maxItems = maxItems;
              }
              if (tagMappings.length) {
                ltv.tagMappings = tagMappings;
              }
              t.hexLtvRepeating = ltv;
            }
            templates.push(t);
          }
        }
        if (templates.length) {
          transportTcpDataTypeConfiguration.protocolTemplates = templates;
        }
        const commands: ProtocolTemplateCommandDefinition[] = [];
        const cmdRows = (this.protocolCommandsArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
        const tplRow0 = (this.protocolTemplatesArray?.getRawValue() ?? [])[0] as Record<string, unknown> | undefined;
        const defaultTemplateId = tplRow0 ? String(tplRow0['id'] ?? '').trim() : '';
        for (const row of cmdRows) {
          const tid = String(row['templateId'] ?? '').trim() || defaultTemplateId;
          if (!tid) {
            continue;
          }
          const cmd: ProtocolTemplateCommandDefinition = {
            templateId: tid,
            commandValue: Number(row['commandValue']) || 0,
            matchValueType: row['matchValueType'] as TcpHexValueType,
            direction: row['direction'] as ProtocolTemplateCommandDirection
          };
          const secOff = this.optionalFormNumber(row['secondaryMatchByteOffset']);
          if (secOff !== undefined && secOff >= 0) {
            cmd.secondaryMatchByteOffset = secOff;
            cmd.secondaryMatchValueType = (row['secondaryMatchValueType'] as TcpHexValueType) ?? TcpHexValueType.UINT8;
            const sm = this.optionalFormNumber(row['secondaryMatchValue']);
            if (sm !== undefined) {
              cmd.secondaryMatchValue = sm;
            }
          }
          const cn = String(row['name'] ?? '').trim();
          if (cn) {
            cmd.name = cn;
          }
          const overrideRows = (row['overrideFields'] as Array<Record<string, unknown>>) ?? [];
          const overrides = this.fieldsFromHexRows(overrideRows);
          if (overrides.length) {
            cmd.fields = overrides;
          }
          commands.push(cmd);
        }
        if (commands.length) {
          transportTcpDataTypeConfiguration.protocolCommands = commands;
        }
        const bundleId = v.protocolTemplateBundleId;
        if (bundleId) {
          transportTcpDataTypeConfiguration.protocolTemplateBundleId = bundleId;
        }
      } else {
        transportTcpDataTypeConfiguration.transportTcpDataType = TransportTcpDataType.HEX;
        const cmdRows = (this.hexCommandProfilesArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
        const profiles: TcpHexCommandProfile[] = [];
        for (const row of cmdRows) {
          const fieldRows = (row['fields'] as Array<Record<string, unknown>>) ?? [];
          const fields = this.fieldsFromHexRows(fieldRows);
          if (fields.length === 0) {
            continue;
          }
          const name = String(row['name'] ?? '').trim();
          const prof: TcpHexCommandProfile = {
            ...(name ? {name} : {}),
            matchByteOffset: Number(row['matchByteOffset']) || 0,
            matchValueType: row['matchValueType'] as TcpHexValueType,
            matchValue: Number(row['matchValue']) || 0,
            fields
          };
          const sOff = this.optionalFormNumber(row['secondaryMatchByteOffset']);
          if (sOff !== undefined && sOff >= 0) {
            prof.secondaryMatchByteOffset = sOff;
            prof.secondaryMatchValueType = (row['secondaryMatchValueType'] as TcpHexValueType) ?? TcpHexValueType.UINT8;
            const sm = this.optionalFormNumber(row['secondaryMatchValue']);
            if (sm !== undefined) {
              prof.secondaryMatchValue = sm;
            }
          }
          profiles.push(prof);
        }
        if (profiles.length) {
          transportTcpDataTypeConfiguration.hexCommandProfiles = profiles;
        }
        const defaultRows = (this.hexProtocolFieldsArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
        const defaultFields = this.fieldsFromHexRows(defaultRows);
        if (defaultFields.length) {
          transportTcpDataTypeConfiguration.hexProtocolFields = defaultFields;
        }
        if (v.hexLtvEnabled) {
          const ltvTagRows = (this.hexLtvTagMappingsArray?.getRawValue() ?? []) as Array<Record<string, unknown>>;
          const tagMappings = this.ltvTagMappingsFromRows(ltvTagRows);
          const ltv: TcpHexLtvRepeatingConfig = {
            startByteOffset: Number(v.hexLtvStartOffset) || 0,
            lengthFieldType: v.hexLtvLengthType,
            tagFieldType: v.hexLtvTagType,
            chunkOrder: v.hexLtvChunkOrder,
            keyPrefix: String(v.hexLtvKeyPrefix || 'ltv').trim() || 'ltv',
            unknownTagMode: v.hexLtvUnknownMode
          };
          const maxItems = this.optionalFormNumber(v.hexLtvMaxItems);
          if (maxItems !== undefined) {
            ltv.maxItems = maxItems;
          }
          if (tagMappings.length) {
            ltv.tagMappings = tagMappings;
          }
          transportTcpDataTypeConfiguration.hexLtvRepeating = ltv;
        }
        const manualCs = String(v.hexChecksumType ?? 'NONE').trim();
        if (manualCs && manualCs.toUpperCase() !== 'NONE') {
          transportTcpDataTypeConfiguration.checksum = {
            type: manualCs,
            fromByte: Number(v.hexChecksumFromByte) || 0,
            toExclusive: Number(v.hexChecksumToExclusive) || 0,
            checksumByteIndex: Number(v.hexChecksumByteIndex) ?? -2
          };
        }
      }
    } else {
      transportTcpDataTypeConfiguration.transportTcpDataType = v.dataType;
    }
    const configuration: TcpDeviceProfileTransportConfiguration = {
      tcpTransportConnectMode: v.tcpTransportConnectMode,
      tcpTransportFramingMode: v.tcpTransportFramingMode,
      tcpWireAuthenticationMode: v.tcpWireAuthenticationMode,
      tcpJsonWithoutMethodMode: v.tcpJsonWithoutMethodMode,
      tcpOpaqueRuleEngineKey: v.tcpOpaqueRuleEngineKey,
      transportTcpDataTypeConfiguration,
      type: DeviceTransportType.TCP
    };
    if (v.tcpTransportFramingMode === TcpTransportFramingMode.FIXED_LENGTH) {
      configuration.tcpFixedFrameLength = v.tcpFixedFrameLength;
    } else {
      configuration.tcpFixedFrameLength = null;
    }
    if (v.tcpOutboundReconnectIntervalSec != null && v.tcpOutboundReconnectIntervalSec !== '') {
      configuration.tcpOutboundReconnectIntervalSec = Number(v.tcpOutboundReconnectIntervalSec);
    }
    if (v.tcpOutboundReconnectMaxAttempts != null && v.tcpOutboundReconnectMaxAttempts !== '') {
      configuration.tcpOutboundReconnectMaxAttempts = Number(v.tcpOutboundReconnectMaxAttempts);
    }
    if (v.tcpReadIdleTimeoutSec != null && v.tcpReadIdleTimeoutSec !== '') {
      configuration.tcpReadIdleTimeoutSec = Number(v.tcpReadIdleTimeoutSec);
    }
    this.propagateChange(configuration);
  }

  /** matInput 可能为 string / number；空字符串视为未填。 */
  private optionalFormNumber(value: unknown): number | undefined {
    if (value === null || value === undefined || value === '') {
      return undefined;
    }
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }

  private ltvTagMappingsFromRows(rows: Array<Record<string, unknown>>): TcpHexLtvTagMapping[] {
    return rows
      .filter(r => r['telemetryKey'] && String(r['telemetryKey']).trim())
      .map(r => {
        const m: TcpHexLtvTagMapping = {
          tagValue: Number(r['tagValue']) || 0,
          telemetryKey: String(r['telemetryKey']).trim(),
          valueType: r['valueType'] as TcpHexValueType
        };
        const byteLength = this.optionalFormNumber(r['byteLength']);
        if (byteLength !== undefined) {
          m.byteLength = byteLength;
        }
        return m;
      });
  }

  private fieldsFromHexRows(rows: Array<Record<string, unknown>>): TcpHexFieldDefinition[] {
    return rows
      .filter(r => r['key'] && String(r['key']).trim())
      .map(r => {
        const def: TcpHexFieldDefinition = {
          key: String(r['key']).trim(),
          byteOffset: Number(r['byteOffset']) || 0,
          valueType: r['valueType'] as TcpHexValueType
        };
        const vt = def.valueType;
        if (vt === TcpHexValueType.BYTES_AS_HEX) {
          const mode = String(r['hexFieldLengthMode'] ?? 'fixed').trim();
          if (mode === 'fromFrame') {
            const blFrom = this.optionalFormNumber(r['byteLengthFromByteOffset']);
            if (blFrom !== undefined) {
              def.byteLengthFromByteOffset = blFrom;
            }
            if (r['byteLengthFromValueType']) {
              def.byteLengthFromValueType = r['byteLengthFromValueType'] as TcpHexValueType;
            }
          } else {
            const bl = this.optionalFormNumber(r['byteLength']);
            if (bl !== undefined) {
              def.byteLength = bl;
            }
          }
        }
        return def;
      });
  }

  validate(): ValidationErrors | null {
    if (!this.tcpDeviceProfileTransportConfigurationFormGroup.valid) {
      return {tcpProfileTransport: true};
    }
    const v = this.tcpDeviceProfileTransportConfigurationFormGroup.getRawValue();
    const dataType = v.dataType;
    const usePt = this.usesProtocolTemplatePayload(v as Record<string, unknown>);
    if (dataType === TransportTcpDataType.HEX && !usePt) {
      const checkBytesAsHex = (g: UntypedFormGroup) => {
        const key = g.get('key')?.value;
        if (!key || !String(key).trim()) {
          return null;
        }
        if (g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX) {
          const mode = String(g.get('hexFieldLengthMode')?.value ?? 'fixed').trim();
          if (mode === 'fromFrame') {
            const fromOff = g.get('byteLengthFromByteOffset')?.value;
            const hasFrom = fromOff != null && fromOff !== '' && Number(fromOff) >= 0;
            if (!hasFrom) {
              return {tcpHexProtocolByteLength: true};
            }
          } else {
            const bl = g.get('byteLength')?.value;
            const hasFixed = bl != null && bl !== '' && Number(bl) >= 1;
            if (!hasFixed) {
              return {tcpHexProtocolByteLength: true};
            }
          }
        }
        return null;
      };
      for (let i = 0; i < this.hexProtocolFieldsArray.length; i++) {
        const err = checkBytesAsHex(this.hexProtocolFieldsArray.at(i) as UntypedFormGroup);
        if (err) {
          return err;
        }
      }
      for (let ci = 0; ci < this.hexCommandProfilesArray.length; ci++) {
        const fieldsArr = this.getCommandFieldsArray(ci);
        for (let fi = 0; fi < fieldsArr.length; fi++) {
          const err = checkBytesAsHex(fieldsArr.at(fi) as UntypedFormGroup);
          if (err) {
            return err;
          }
        }
      }
      if (this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvEnabled')?.value) {
        for (let li = 0; li < this.hexLtvTagMappingsArray.length; li++) {
          const g = this.hexLtvTagMappingsArray.at(li) as UntypedFormGroup;
          const tk = g.get('telemetryKey')?.value;
          if (!tk || !String(tk).trim()) {
            continue;
          }
          if (g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX) {
            const bl = g.get('byteLength')?.value;
            if (bl == null || bl === '' || Number(bl) < 1) {
              return {tcpHexLtvByteLength: true};
            }
          }
        }
      }
    }
    if (dataType === TransportTcpDataType.HEX && usePt) {
      const checkBytesAsHex = (g: UntypedFormGroup) => {
        const key = g.get('key')?.value;
        if (!key || !String(key).trim()) {
          return null;
        }
        if (g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX) {
          const mode = String(g.get('hexFieldLengthMode')?.value ?? 'fixed').trim();
          if (mode === 'fromFrame') {
            const fromOff = g.get('byteLengthFromByteOffset')?.value;
            const hasFrom = fromOff != null && fromOff !== '' && Number(fromOff) >= 0;
            if (!hasFrom) {
              return {tcpHexProtocolByteLength: true};
            }
          } else {
            const bl = g.get('byteLength')?.value;
            const hasFixed = bl != null && bl !== '' && Number(bl) >= 1;
            if (!hasFixed) {
              return {tcpHexProtocolByteLength: true};
            }
          }
        }
        return null;
      };
      for (let ti = 0; ti < this.protocolTemplatesArray.length; ti++) {
        const fieldsArr = this.getProtocolTemplateFieldsArray(ti);
        for (let fi = 0; fi < fieldsArr.length; fi++) {
          const err = checkBytesAsHex(fieldsArr.at(fi) as UntypedFormGroup);
          if (err) {
            return err;
          }
        }
        const tpl = this.protocolTemplatesArray.at(ti) as UntypedFormGroup;
        if (tpl.get('hexLtvEnabled')?.value) {
          const ltvArr = tpl.get('hexLtvTagMappings') as UntypedFormArray;
          for (let li = 0; li < ltvArr.length; li++) {
            const g = ltvArr.at(li) as UntypedFormGroup;
            const tk = g.get('telemetryKey')?.value;
            if (!tk || !String(tk).trim()) {
              continue;
            }
            if (g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX) {
              const bl = g.get('byteLength')?.value;
              if (bl == null || bl === '' || Number(bl) < 1) {
                return {tcpHexLtvByteLength: true};
              }
            }
          }
        }
      }
      for (let ci = 0; ci < this.protocolCommandsArray.length; ci++) {
        const ov = this.getProtocolCommandOverrideFieldsArray(ci);
        for (let fi = 0; fi < ov.length; fi++) {
          const err = checkBytesAsHex(ov.at(fi) as UntypedFormGroup);
          if (err) {
            return err;
          }
        }
      }
    }
    return null;
  }
}
