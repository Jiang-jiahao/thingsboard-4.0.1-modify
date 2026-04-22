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
import { ChangeDetectorRef, Component, forwardRef, Input, AfterViewInit, OnDestroy, OnInit } from '@angular/core';
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
  fixedBytesHexModelToFormControl,
  formatFixedWireIntegralFromModel,
  formatTcpHexMatchValueHexHint,
  ltvTagWireTextForMapping,
  normalizeFixedBytesHexWhitespace,
  parseIntegralWireTextToNumber,
  parseLtvTagWireTextToNumber,
  unescapeCStyleForFixedUtf8String,
  unknownTagTelemetryKeyHexLiteralFromMappings
} from '@home/pages/profiles/protocol-template-downlink-fields.util';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormArray,
  AbstractControl,
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
  TCP_HEX_FRAME_FIELD_VALUE_TYPES,
  TCP_HEX_MATCH_VALUE_TYPES,
  TCP_HEX_PROTOCOL_TEMPLATE_COMMAND_MATCH_TYPES,
  TcpHexCommandProfile,
  TcpHexFieldDefinition,
  TcpHexLtvChunkOrder,
  TcpHexLtvRepeatingConfig,
  TcpHexLtvTagMapping,
  TcpHexUnknownTagMode,
  TcpHexValueType,
  isTcpHexVariableByteSlice,
  TCP_HEX_LTV_TAG_VALUE_OPTIONS,
  migrateLegacyLtvTagValueType,
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
export class TcpDeviceProfileTransportConfigurationComponent implements OnInit, AfterViewInit, OnDestroy, ControlValueAccessor, Validator {
  /** 手动 HEX 下「LTV/TLV 重复段」折叠面板：已配置 LTV 或用户勾选启用时默认展开 */
  hexLtvDefaultPathPanelExpanded = false;

  tcpTransportFramingModes = Object.values(TcpTransportFramingMode);
  /** 显式列出，避免部分环境下 Object.values(enum) 与下拉展示不一致 */
  transportTcpDataTypes: TransportTcpDataType[] = [
    TransportTcpDataType.JSON,
    TransportTcpDataType.HEX,
    TransportTcpDataType.ASCII
  ];
  tcpHexValueTypes = TCP_HEX_FRAME_FIELD_VALUE_TYPES;
  /** LTV Tag→遥测映射下拉（静态 labelKey，供 translate 使用） */
  tcpHexLtvTagValueOptions = TCP_HEX_LTV_TAG_VALUE_OPTIONS;
  tcpHexMatchValueTypes = TCP_HEX_MATCH_VALUE_TYPES;
  tcpHexCommandMatchValueTypes = TCP_HEX_PROTOCOL_TEMPLATE_COMMAND_MATCH_TYPES;
  readonly TransportTcpDataType = TransportTcpDataType;
  readonly TcpHexValueType = TcpHexValueType;
  readonly TcpHexLtvChunkOrder = TcpHexLtvChunkOrder;
  readonly TcpHexUnknownTagMode = TcpHexUnknownTagMode;
  tcpDeviceProfileTransportConfigurationFormGroup: UntypedFormGroup;
  private destroy$ = new Subject<void>();
  private readonly ltvTagWireTextValidator = (control: AbstractControl): ValidationErrors | null => {
    const v = parseLtvTagWireTextToNumber(control.value);
    return v === undefined ? { ltvTagInvalid: true } : null;
  };
  @Input()
  disabled: boolean;

  /** 租户「协议模板」库，用于协议模板负载时下拉选择 */
  protocolTemplateBundles: ProtocolTemplateBundle[] = [];

  private propagateChange = (v: any) => {
  };
  constructor(
    private fb: UntypedFormBuilder,
    private store: Store<AppState>,
    private protocolTemplateBundleService: ProtocolTemplateBundleService,
    private cdr: ChangeDetectorRef
  ) {
  }

  ngAfterViewInit(): void {
    this.scheduleSyncHexLtvPanelExpanded();
  }

  /** 手动 HEX 区可能晚于 writeValue 才渲染（*ngIf）；延迟再同步一次展开状态 */
  private scheduleSyncHexLtvPanelExpanded(): void {
    const sync = () => {
      if (this.tcpDeviceProfileTransportConfigurationFormGroup?.get('hexLtvEnabled')?.value) {
        this.hexLtvDefaultPathPanelExpanded = true;
        this.cdr.markForCheck();
      }
    };
    queueMicrotask(sync);
    setTimeout(sync, 0);
    setTimeout(sync, 50);
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
      hexLtvLengthIncludesTag: [false],
      hexLtvLengthIncludesLengthField: [false],
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
    this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvEnabled').valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe((en: boolean) => {
      if (en) {
        this.hexLtvDefaultPathPanelExpanded = true;
        this.cdr.markForCheck();
      }
    });
    this.tcpDeviceProfileTransportConfigurationFormGroup.get('protocolTemplateBundleId').valueChanges.pipe(
      takeUntil(this.destroy$)
    ).subscribe(() => {
      this.scheduleSyncHexLtvPanelExpanded();
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

  /** 手动 HEX 默认字段区：按字节偏移升序展示；下标仍为 FormArray 真实索引 */
  hexProtocolFieldIndicesSortedByByteOffset(): number[] {
    const fa = this.hexProtocolFieldsArray;
    const n = fa.length;
    return Array.from({ length: n }, (_, i) => i).sort((a, b) => {
      const da = this.hexFieldByteOffsetAt(fa, a);
      const db = this.hexFieldByteOffsetAt(fa, b);
      if (da !== db) {
        return da - db;
      }
      return a - b;
    });
  }

  private hexFieldByteOffsetAt(fa: UntypedFormArray, i: number): number {
    const v = (fa.at(i) as UntypedFormGroup).get('byteOffset')?.value;
    const num = Number(v);
    return Number.isFinite(num) ? num : Number.MAX_SAFE_INTEGER;
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

  private createLtvTagMappingGroup(
    m?: TcpHexLtvTagMapping,
    tagFieldType?: TcpHexValueType,
    sectionUnknownTagTelemetryKeyHexLiteral?: boolean | null
  ): UntypedFormGroup {
    const vt = tagFieldType ?? TcpHexValueType.UINT8;
    return this.fb.group({
      tagValue: [ltvTagWireTextForMapping(m, vt, sectionUnknownTagTelemetryKeyHexLiteral),
        [Validators.required, this.ltvTagWireTextValidator]],
      telemetryKey: [m?.telemetryKey ?? '', [Validators.maxLength(255)]],
      valueType: [migrateLegacyLtvTagValueType(m?.valueType), Validators.required]
    });
  }

  private patchHexLtvFromModel(cfg: TcpHexLtvRepeatingConfig | undefined) {
    const arr = this.hexLtvTagMappingsArray;
    while (arr.length) {
      arr.removeAt(0, {emitEvent: false});
    }
    const tagFt = cfg?.tagFieldType ?? TcpHexValueType.UINT8;
    if (cfg?.tagMappings?.length) {
      for (const m of cfg.tagMappings) {
        arr.push(this.createLtvTagMappingGroup(m, tagFt, cfg?.unknownTagTelemetryKeyHexLiteral), {emitEvent: false});
      }
    }
    if (cfg) {
      this.hexLtvDefaultPathPanelExpanded = true;
    }
    this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({
      hexLtvEnabled: !!cfg,
      hexLtvStartOffset: cfg?.startByteOffset ?? 0,
      hexLtvLengthType: cfg?.lengthFieldType ?? TcpHexValueType.UINT8,
      hexLtvTagType: cfg?.tagFieldType ?? TcpHexValueType.UINT8,
      hexLtvChunkOrder: cfg?.chunkOrder ?? TcpHexLtvChunkOrder.LTV,
      hexLtvMaxItems: cfg?.maxItems ?? 32,
      hexLtvKeyPrefix: cfg?.keyPrefix ?? 'ltv',
      hexLtvUnknownMode: cfg?.unknownTagMode ?? TcpHexUnknownTagMode.SKIP,
      hexLtvLengthIncludesTag: !!cfg?.lengthIncludesTag,
      hexLtvLengthIncludesLengthField: !!cfg?.lengthIncludesLengthField
    }, {emitEvent: false});
  }

  private patchHexLtvDisabled() {
    this.patchHexLtvFromModel(undefined);
    this.tcpDeviceProfileTransportConfigurationFormGroup.patchValue({hexLtvEnabled: false}, {emitEvent: false});
    this.hexLtvDefaultPathPanelExpanded = false;
  }

  addHexLtvTagMappingRow() {
    const vt = this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvTagType')?.value as TcpHexValueType;
    this.hexLtvTagMappingsArray.push(this.createLtvTagMappingGroup(undefined, vt));
    this.updateModel();
  }

  removeHexLtvTagMappingRow(index: number) {
    this.hexLtvTagMappingsArray.removeAt(index);
    this.updateModel();
  }

  /** 支持 `0x` 或十进制；失焦后按输入风格规范为 `0x` 小写或十进制字符串 */
  onLtvTagValueBlur(li: number): void {
    if (this.disabled) {
      return;
    }
    const g = this.hexLtvTagMappingsArray.at(li) as UntypedFormGroup;
    const ctrl = g.get('tagValue');
    const t = String(ctrl?.value ?? '');
    const trimmed = t.trim().replace(/\s+/g, '');
    if (!trimmed) {
      return;
    }
    const n = parseLtvTagWireTextToNumber(t);
    if (n === undefined) {
      return;
    }
    const vt = (this.tcpDeviceProfileTransportConfigurationFormGroup.get('hexLtvTagType')?.value ??
      TcpHexValueType.UINT8) as TcpHexValueType;
    if (/^0x/i.test(trimmed)) {
      ctrl?.patchValue(formatTcpHexMatchValueHexHint(n, vt), { emitEvent: false });
    } else {
      ctrl?.patchValue(String(n), { emitEvent: false });
    }
    this.updateModel();
  }

  isBytesAsHexCommandRow(commandIndex: number, fieldIndex: number): boolean {
    const g = this.getCommandFieldsArray(commandIndex).at(fieldIndex) as UntypedFormGroup;
    return isTcpHexVariableByteSlice(g.get('valueType')?.value as TcpHexValueType);
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
      byteLengthFromValueType: [f?.byteLengthFromValueType ?? TcpHexValueType.UINT8, Validators.required],
      byteLengthFromIntegralSubtract: [f?.byteLengthFromIntegralSubtract ?? null, [Validators.min(0)]],
      fixedWireIntegralValueText: [formatFixedWireIntegralFromModel(f?.fixedWireIntegralValue)],
      fixedBytesHex: [fixedBytesHexModelToFormControl(f)]
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
    const ltvTagFt = ltv?.tagFieldType ?? TcpHexValueType.UINT8;
    if (ltv?.tagMappings?.length) {
      for (const m of ltv.tagMappings) {
        ltvArr.push(this.createLtvTagMappingGroup(m, ltvTagFt, ltv?.unknownTagTelemetryKeyHexLiteral));
      }
    }
    return this.fb.group({
      id: [t?.id ?? '', [Validators.required, Validators.maxLength(255)]],
      name: [t?.name ?? '', [Validators.maxLength(255)]],
      commandByteOffset: [t?.commandByteOffset ?? 12, [Validators.required, Validators.min(0)]],
      commandMatchWidth: [t?.commandMatchWidth === 1 ? 1 : 4],
      hexProtocolFields: fieldsArr,
      hexLtvEnabled: [!!ltv],
      hexLtvStartOffset: [ltv?.startByteOffset ?? 0, [Validators.required, Validators.min(0)]],
      hexLtvLengthType: [ltv?.lengthFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvTagType: [ltv?.tagFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvChunkOrder: [ltv?.chunkOrder ?? TcpHexLtvChunkOrder.LTV, Validators.required],
      hexLtvMaxItems: [ltv?.maxItems ?? 32, [Validators.min(1)]],
      hexLtvKeyPrefix: [ltv?.keyPrefix ?? 'ltv'],
      hexLtvUnknownMode: [ltv?.unknownTagMode ?? TcpHexUnknownTagMode.SKIP, Validators.required],
      hexLtvLengthIncludesTag: [!!ltv?.lengthIncludesTag],
      hexLtvLengthIncludesLengthField: [!!ltv?.lengthIncludesLengthField],
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
    const matchVt = c?.matchValueType ?? TcpHexValueType.UINT32_LE;
    const primaryWire = isTcpHexVariableByteSlice(matchVt)
      ? (c?.commandMatchBytesHex ? normalizeFixedBytesHexWhitespace(c.commandMatchBytesHex) : '')
      : this.formatProtocolCommandIntegralForForm(c?.commandValue);
    return this.fb.group({
      templateId: [c?.templateId ?? '', Validators.required],
      name: [c?.name ?? '', [Validators.maxLength(255)]],
      commandValue: [primaryWire, Validators.required],
      matchValueType: [matchVt, Validators.required],
      secondaryMatchByteOffset: [c?.secondaryMatchByteOffset ?? null],
      secondaryMatchValueType: [c?.secondaryMatchValueType ?? TcpHexValueType.UINT8],
      secondaryMatchValue: [c?.secondaryMatchValue ?? null],
      direction: [c?.direction ?? ProtocolTemplateCommandDirection.UPLINK, Validators.required],
      overrideFields: overrideArr
    });
  }

  /** 与帧模板「固定线值整型」一致：十进制回显 */
  private formatProtocolCommandIntegralForForm(v: number | undefined | null): string {
    const s = formatFixedWireIntegralFromModel(v);
    return s === '' ? '0' : s;
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
    const vt = tpl.get('hexLtvTagType')?.value as TcpHexValueType;
    (tpl.get('hexLtvTagMappings') as UntypedFormArray).push(this.createLtvTagMappingGroup(undefined, vt));
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
    return isTcpHexVariableByteSlice(g.get('valueType')?.value as TcpHexValueType);
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
      g.patchValue({
        byteLengthFromByteOffset: null,
        byteLengthFromValueType: TcpHexValueType.UINT8,
        byteLengthFromIntegralSubtract: null
      }, { emitEvent: false });
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
      this.scheduleSyncHexLtvPanelExpanded();
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
              commandMatchWidth: Number(row['commandMatchWidth']) === 1 ? 1 : 4
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
              if (row['hexLtvLengthIncludesTag'] === true || row['hexLtvLengthIncludesTag'] === 'true' || row['hexLtvLengthIncludesTag'] === 1) {
                ltv.lengthIncludesTag = true;
              }
              if (row['hexLtvLengthIncludesLengthField'] === true || row['hexLtvLengthIncludesLengthField'] === 'true' || row['hexLtvLengthIncludesLengthField'] === 1) {
                ltv.lengthIncludesLengthField = true;
              }
              if (tagMappings.length) {
                ltv.tagMappings = tagMappings;
              }
              ltv.unknownTagTelemetryKeyHexLiteral = unknownTagTelemetryKeyHexLiteralFromMappings(tagMappings);
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
          const matchVt = row['matchValueType'] as TcpHexValueType;
          const intCmd = parseIntegralWireTextToNumber(String(row['commandValue'] ?? ''));
          const cmd: ProtocolTemplateCommandDefinition = {
            templateId: tid,
            commandValue: isTcpHexVariableByteSlice(matchVt) ? 0 : (intCmd !== undefined ? Math.trunc(intCmd) : 0),
            matchValueType: matchVt,
            direction: row['direction'] as ProtocolTemplateCommandDirection
          };
          if (isTcpHexVariableByteSlice(matchVt)) {
            const hx = normalizeFixedBytesHexWhitespace(row['commandValue']);
            if (hx) {
              cmd.commandMatchBytesHex = hx;
            }
          }
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
          if (v.hexLtvLengthIncludesTag === true || v.hexLtvLengthIncludesTag === 'true' || v.hexLtvLengthIncludesTag === 1) {
            ltv.lengthIncludesTag = true;
          }
          if (v.hexLtvLengthIncludesLengthField === true || v.hexLtvLengthIncludesLengthField === 'true' || v.hexLtvLengthIncludesLengthField === 1) {
            ltv.lengthIncludesLengthField = true;
          }
          if (tagMappings.length) {
            ltv.tagMappings = tagMappings;
          }
          ltv.unknownTagTelemetryKeyHexLiteral = unknownTagTelemetryKeyHexLiteralFromMappings(tagMappings);
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
          tagValue: parseLtvTagWireTextToNumber(r['tagValue']) ?? 0,
          tagValueLiterallyHex: /^0x/i.test(String(r['tagValue'] ?? '').trim()),
          telemetryKey: String(r['telemetryKey']).trim(),
          valueType: migrateLegacyLtvTagValueType(r['valueType'] as TcpHexValueType)
        };
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
        if (isTcpHexVariableByteSlice(vt)) {
          const mode = String(r['hexFieldLengthMode'] ?? 'fixed').trim();
          if (mode === 'fromFrame') {
            const blFrom = this.optionalFormNumber(r['byteLengthFromByteOffset']);
            if (blFrom !== undefined) {
              def.byteLengthFromByteOffset = blFrom;
            }
            if (r['byteLengthFromValueType']) {
              def.byteLengthFromValueType = r['byteLengthFromValueType'] as TcpHexValueType;
            }
            const subLen = this.optionalFormNumber(r['byteLengthFromIntegralSubtract']);
            if (subLen !== undefined && subLen >= 0) {
              def.byteLengthFromIntegralSubtract = subLen;
            }
          } else {
            const bl = this.optionalFormNumber(r['byteLength']);
            if (bl !== undefined) {
              def.byteLength = bl;
            }
          }
        }
        // 与 TcpHexFieldDefinition.validate 一致：整型固定线值与变长字节切片固定 hex 不能同时存在
        if (isTcpHexVariableByteSlice(vt)) {
          let fixHex: string;
          if (vt === TcpHexValueType.BYTES_AS_UTF8) {
            fixHex = unescapeCStyleForFixedUtf8String(String(r['fixedBytesHex'] ?? '').trim());
          } else {
            fixHex = normalizeFixedBytesHexWhitespace(r['fixedBytesHex']);
          }
          if (fixHex) {
            def.fixedBytesHex = fixHex;
          }
        } else {
          const fixInt = this.parseOptionalIntegralWireText(r['fixedWireIntegralValueText']);
          if (fixInt !== undefined) {
            def.fixedWireIntegralValue = fixInt;
          }
        }
        return def;
      });
  }

  private parseOptionalIntegralWireText(raw: unknown): number | undefined {
    return parseIntegralWireTextToNumber(raw);
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
        if (isTcpHexVariableByteSlice(g.get('valueType')?.value as TcpHexValueType)) {
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
    }
    if (dataType === TransportTcpDataType.HEX && usePt) {
      const checkBytesAsHex = (g: UntypedFormGroup) => {
        const key = g.get('key')?.value;
        if (!key || !String(key).trim()) {
          return null;
        }
        if (isTcpHexVariableByteSlice(g.get('valueType')?.value as TcpHexValueType)) {
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
