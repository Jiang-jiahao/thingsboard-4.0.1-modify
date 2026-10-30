import { Component, Input, OnDestroy } from '@angular/core';
import {
  AbstractControl,
  UntypedFormArray,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validators
} from '@angular/forms';
import {
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateUplinkDataDestination,
  ProtocolTemplateDefinition,
  TcpHexChecksumDefinition,
  TcpHexFieldDefinition,
  TcpHexLtvChunkOrder,
  TcpHexLtvTagMapping,
  TcpHexUnknownTagMode,
  TcpHexValueType,
  TCP_HEX_FRAME_FIELD_VALUE_TYPES,
  TCP_HEX_LTV_TAG_VALUE_OPTIONS,
  migrateLegacyLtvTagValueType,
  isTcpHexVariableByteSlice
} from '@shared/models/device.models';
import { Subscription } from 'rxjs';
import {
  fixedBytesHexModelToFormControl,
  formatFixedWireIntegralFromModel,
  ltvTagWireTextForMapping,
  parseLtvTagWireTextToNumber,
  normalizeFixedBytesHexWhitespace,
  parseIntegralWireTextToNumber
} from '@home/pages/profiles/protocol-template-downlink-fields.util';

@Component({
  selector: 'jnks-iot-protocol-template-bundle-editor',
  templateUrl: './protocol-template-bundle-editor.component.html',
  styleUrls: ['./protocol-template-bundle-editor.component.scss']
})
export class ProtocolTemplateBundleEditorComponent implements OnDestroy {

  @Input()
  protocolTemplateFormGroup: UntypedFormGroup;

  @Input()
  disabled: boolean;

  @Input()
  layoutMode: 'full' | 'templatesOnly' | 'commandsOnly' = 'full';

  readonly TcpHexValueType = TcpHexValueType;
  readonly ProtocolTemplateUplinkDataDestination = ProtocolTemplateUplinkDataDestination;

  tcpHexValueTypes = TCP_HEX_FRAME_FIELD_VALUE_TYPES;
  tcpHexLtvTagValueOptions = TCP_HEX_LTV_TAG_VALUE_OPTIONS;

  private templateIdSubscriptions: Subscription[] = [];

  private readonly ltvTagWireTextValidator = (control: AbstractControl): ValidationErrors | null => {
    const v = parseLtvTagWireTextToNumber(control.value);
    return v === undefined ? { ltvTagInvalid: true } : null;
  };

  constructor(
    private fb: UntypedFormBuilder
  ) {
  }

  ngOnDestroy(): void {
    this.templateIdSubscriptions.forEach(s => s.unsubscribe());
    this.templateIdSubscriptions = [];
  }

  /**
   * 单帧模板：模板 ID 由对话顶部「名称」决定、命令的 templateId 随之同步。
   * 多帧模板：每个模板各自有 ID，命令用下拉选择归属。
   */
  get singleTemplateMode(): boolean {
    return (this.protocolTemplatesArray?.length ?? 0) <= 1;
  }

  /**
   * 与对话框顶部「名称」同步：作为唯一展示名，并写入隐藏字段 id，供命令 templateId 关联。
   */
  applyBundleDisplayNameAsTemplateId(displayName: string): void {
    const arr = this.protocolTemplatesArray;
    if (!arr || arr.length > 1) {
      return;
    }
    const tid = String(displayName ?? '').trim();
    const g = arr.at(0) as UntypedFormGroup;
    if (!g) {
      return;
    }
    g.get('id')?.patchValue(tid, { emitEvent: true });
  }

  get protocolTemplatesArray(): UntypedFormArray {
    return this.protocolTemplateFormGroup?.get('protocolTemplates') as UntypedFormArray;
  }

  get protocolCommandsArray(): UntypedFormArray {
    return this.protocolTemplateFormGroup?.get('protocolCommands') as UntypedFormArray;
  }

  addProtocolTemplateLtvMapping(templateIndex: number): void {
    const tpl = this.protocolTemplatesArray.at(templateIndex) as UntypedFormGroup;
    const vt = tpl.get('hexLtvTagType')?.value as TcpHexValueType;
    (tpl.get('hexLtvTagMappings') as UntypedFormArray).push(this.createLtvTagMappingGroup(undefined, vt));
  }

  removeProtocolTemplateLtvMapping(payload: { templateIndex: number; index: number }): void {
    const tpl = this.protocolTemplatesArray.at(payload.templateIndex) as UntypedFormGroup;
    (tpl.get('hexLtvTagMappings') as UntypedFormArray).removeAt(payload.index);
  }

  addFieldToProtocolTemplate(templateIndex: number) {
    this.getProtocolTemplateFieldsArray(templateIndex).push(this.createHexFieldGroup());
  }

  removeFieldFromProtocolTemplate(payload: { templateIndex: number; fieldIndex: number }) {
    this.getProtocolTemplateFieldsArray(payload.templateIndex).removeAt(payload.fieldIndex);
  }

  getProtocolTemplateFieldsArray(templateIndex: number): UntypedFormArray {
    return (this.protocolTemplatesArray.at(templateIndex) as UntypedFormGroup).get('hexProtocolFields') as UntypedFormArray;
  }

  addProtocolCommand() {
    this.protocolCommandsArray.push(this.createProtocolCommandGroup());
    this.syncCommandTemplateIdsFromFirstTemplate();
  }

  removeProtocolCommand(index: number) {
    this.protocolCommandsArray.removeAt(index);
  }

  addOverrideFieldToCommand(commandIndex: number) {
    this.getProtocolCommandOverrideFieldsArray(commandIndex).push(this.createHexFieldGroup());
  }

  removeOverrideFieldFromCommand(payload: { commandIndex: number; fieldIndex: number }) {
    this.getProtocolCommandOverrideFieldsArray(payload.commandIndex).removeAt(payload.fieldIndex);
  }

  getProtocolCommandOverrideFieldsArray(commandIndex: number): UntypedFormArray {
    return (this.protocolCommandsArray.at(commandIndex) as UntypedFormGroup).get('overrideFields') as UntypedFormArray;
  }

  addProtocolTemplate(): void {
    const arr = this.protocolTemplatesArray;
    if (!arr) {
      return;
    }
    const used = new Set(arr.controls
      .map(c => String((c as UntypedFormGroup).get('id')?.value ?? '').trim()));
    let n = arr.length + 1;
    let id = `template${n}`;
    while (used.has(id)) {
      n += 1;
      id = `template${n}`;
    }
    arr.push(this.createProtocolTemplateGroup({ id } as ProtocolTemplateDefinition));
    this.wireTemplateIdSync();
    this.protocolTemplateFormGroup?.updateValueAndValidity({ emitEvent: true });
  }

  removeProtocolTemplate(index: number): void {
    const arr = this.protocolTemplatesArray;
    if (!arr || arr.length <= 1) {
      return;
    }
    const i = Math.min(Math.max(0, Number(index) || 0), arr.length - 1);
    const removedId = String((arr.at(i) as UntypedFormGroup).get('id')?.value ?? '').trim();
    arr.removeAt(i);
    // 被删模板名下的命令改指剩下的第一个模板，避免留下悬空 templateId
    const fallbackId = String((arr.at(0) as UntypedFormGroup).get('id')?.value ?? '').trim();
    if (removedId && fallbackId) {
      for (let c = 0; c < this.protocolCommandsArray.length; c++) {
        const ctl = (this.protocolCommandsArray.at(c) as UntypedFormGroup).get('templateId');
        if (ctl && String(ctl.value ?? '').trim() === removedId) {
          ctl.patchValue(fallbackId, { emitEvent: false });
        }
      }
    }
    this.wireTemplateIdSync();
    this.protocolTemplateFormGroup?.updateValueAndValidity({ emitEvent: true });
  }

  private createHexFieldGroup(f?: TcpHexFieldDefinition): UntypedFormGroup {
    const fromFrame = f?.byteLengthFromByteOffset != null && f.byteLengthFromByteOffset >= 0;
    return this.fb.group({
      key: [f?.key ?? '', [Validators.maxLength(255)]],
      byteOffset: [f?.byteOffset ?? 0, [Validators.required, Validators.min(0)]],
      valueType: [f?.valueType ?? TcpHexValueType.UINT8, Validators.required],
      /** 上行 原始值 × scale，下行 填写值 ÷ scale；留空/0 视为 1 */
      scale: [f?.scale ?? null],
      hexFieldLengthMode: [fromFrame ? 'fromFrame' : 'fixed'],
      byteLength: [f?.byteLength ?? null, [Validators.min(1)]],
      byteLengthFromByteOffset: [f?.byteLengthFromByteOffset ?? null, [Validators.min(0)]],
      byteLengthFromValueType: [f?.byteLengthFromValueType ?? TcpHexValueType.UINT8, Validators.required],
      byteLengthFromIntegralSubtract: [f?.byteLengthFromIntegralSubtract ?? null, [Validators.min(0)]],
      includeInDownlinkPayloadLength: [!!f?.includeInDownlinkPayloadLength],
      autoDownlinkTotalFrameLength: [!!f?.autoDownlinkTotalFrameLength],
      downlinkTotalFrameLengthExcludesLengthFieldBytes: [!!f?.downlinkTotalFrameLengthExcludesLengthFieldBytes],
      fixedWireIntegralValueText: [formatFixedWireIntegralFromModel(f?.fixedWireIntegralValue)],
      fixedBytesHex: [fixedBytesHexModelToFormControl(f)]
    });
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
    const cs: TcpHexChecksumDefinition | undefined = t?.checksum;
    const csType = cs?.type && String(cs.type).trim() && String(cs.type).toUpperCase() !== 'NONE'
      ? String(cs.type).trim()
      : 'NONE';
    return this.fb.group({
      id: [t?.id ?? '', [Validators.required, Validators.maxLength(255)]],
      name: [t?.name ?? '', [Validators.maxLength(255)]],
      commandByteOffset: [t?.commandByteOffset ?? 12, [Validators.required, Validators.min(0)]],
      commandMatchWidth: [t?.commandMatchWidth === 1 ? 1 : 4],
      checksumType: [csType],
      checksumFromByte: [cs?.fromByte ?? 0],
      checksumToExclusive: [cs?.toExclusive ?? 0],
      checksumByteIndex: [cs?.checksumByteIndex ?? -2],
      hexProtocolFields: fieldsArr,
      hexLtvEnabled: [!!ltv],
      hexLtvStartOffset: [ltv?.startByteOffset ?? 0, [Validators.required, Validators.min(0)]],
      hexLtvLengthType: [ltv?.lengthFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvTagType: [ltv?.tagFieldType ?? TcpHexValueType.UINT8, Validators.required],
      hexLtvChunkOrder: [ltv?.chunkOrder ?? TcpHexLtvChunkOrder.LTV, Validators.required],
      hexLtvMaxItems: [ltv?.maxItems ?? 32, [Validators.min(1)]],
      hexLtvKeyPrefix: [ltv?.keyPrefix ?? 'ltv'],
      hexLtvUnknownMode: [ltv?.unknownTagMode ?? TcpHexUnknownTagMode.SKIP, Validators.required],
      hexLtvLengthIncludesLengthField: [!!ltv?.lengthIncludesLengthField],
      hexLtvLengthIncludesTag: [!!ltv?.lengthIncludesLengthField ? false : !!ltv?.lengthIncludesTag],
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
      : this.formatCommandValueForForm(c?.commandValue);
    const secVt = c?.secondaryMatchValueType ?? TcpHexValueType.UINT8;
    const secWire = isTcpHexVariableByteSlice(secVt)
      ? (c?.secondaryMatchBytesHex ? normalizeFixedBytesHexWhitespace(c.secondaryMatchBytesHex) : '')
      : (c?.secondaryMatchValue != null ? this.formatCommandValueForForm(c.secondaryMatchValue) : '');
    return this.fb.group({
      templateId: [c?.templateId ?? '', Validators.required],
      name: [c?.name ?? '', [Validators.maxLength(255)]],
      commandValue: [primaryWire, Validators.required],
      matchValueType: [matchVt, Validators.required],
      secondaryMatchByteOffset: [c?.secondaryMatchByteOffset ?? null],
      secondaryMatchValueType: [secVt],
      secondaryMatchValue: [secWire],
      direction: [c?.direction ?? ProtocolTemplateCommandDirection.UPLINK, Validators.required],
      uplinkDataDestination: [c?.uplinkDataDestination ?? ProtocolTemplateUplinkDataDestination.TELEMETRY],
      downlinkPayloadLengthAuto: [!!c?.downlinkPayloadLengthAuto],
      downlinkPayloadLengthFieldKey: [c?.downlinkPayloadLengthFieldKey ?? ''],
      innerChecksumType: [c?.innerChecksum?.type ?? 'NONE'],
      innerChecksumFromByte: [c?.innerChecksum?.fromByte ?? 0],
      innerChecksumToExclusive: [c?.innerChecksum?.toExclusive ?? 0],
      innerChecksumByteIndex: [c?.innerChecksum?.checksumByteIndex ?? -1],
      overrideFields: overrideArr
    }, { validators: [this.protocolCommandPrimaryWireValidator, this.protocolCommandSecondaryWireValidator] });
  }

  private readonly protocolCommandPrimaryWireValidator = (group: AbstractControl): ValidationErrors | null => {
    const g = group as UntypedFormGroup;
    const vt = g.get('matchValueType')?.value as TcpHexValueType;
    const raw = g.get('commandValue')?.value;
    const s = String(raw ?? '').trim();
    if (!s) {
      return { cmdPrimaryWireRequired: true };
    }
    if (isTcpHexVariableByteSlice(vt)) {
      const h = normalizeFixedBytesHexWhitespace(s).replace(/^0x/i, '');
      if (!h || !/^[0-9a-fA-F]+$/.test(h) || (h.length & 1) === 1) {
        return { cmdPrimaryWireHexInvalid: true };
      }
      return null;
    }
    const n = parseIntegralWireTextToNumber(s);
    return n === undefined ? { cmdPrimaryWireIntegralInvalid: true } : null;
  };

  private readonly protocolCommandSecondaryWireValidator = (group: AbstractControl): ValidationErrors | null => {
    const g = group as UntypedFormGroup;
    const rawOff = g.get('secondaryMatchByteOffset')?.value;
    if (rawOff === null || rawOff === undefined || rawOff === '') {
      return null;
    }
    const offNum = Number(rawOff);
    if (!Number.isFinite(offNum) || offNum < 0) {
      return null;
    }
    const secVt = g.get('secondaryMatchValueType')?.value as TcpHexValueType;
    const raw = g.get('secondaryMatchValue')?.value;
    const s = String(raw ?? '').trim();
    if (!s) {
      return { cmdSecondaryWireRequired: true };
    }
    if (isTcpHexVariableByteSlice(secVt)) {
      const h = normalizeFixedBytesHexWhitespace(s).replace(/^0x/i, '');
      if (!h || !/^[0-9a-fA-F]+$/.test(h) || (h.length & 1) === 1) {
        return { cmdSecondaryWireHexInvalid: true };
      }
      return null;
    }
    const n = parseIntegralWireTextToNumber(s);
    return n === undefined ? { cmdSecondaryWireIntegralInvalid: true } : null;
  };

  patchProtocolTemplatesFromModel(templates: ProtocolTemplateDefinition[]) {
    const arr = this.protocolTemplatesArray;
    while (arr.length) {
      arr.removeAt(0, { emitEvent: false });
    }
    const list = templates?.length ? templates : [undefined];
    for (const t of list) {
      arr.push(this.createProtocolTemplateGroup(t as ProtocolTemplateDefinition | undefined), { emitEvent: false });
    }
    this.wireTemplateIdSync();
    this.syncCommandTemplateIdsFromFirstTemplate();
    this.protocolTemplateFormGroup?.updateValueAndValidity({ emitEvent: true });
  }

  patchProtocolCommandsFromModel(commands: ProtocolTemplateCommandDefinition[]) {
    const arr = this.protocolCommandsArray;
    while (arr.length) {
      arr.removeAt(0, { emitEvent: false });
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
        arr.push(this.createProtocolCommandGroup(merged), { emitEvent: false });
      }
    }
    this.syncCommandTemplateIdsFromFirstTemplate();
    this.protocolTemplateFormGroup?.updateValueAndValidity({ emitEvent: true });
  }

  /**
   * 单模板：模板 ID 变化时把命令的 templateId 一并改掉（归属唯一，无歧义）。
   * 多模板：只把原本指向旧 ID 的命令改到新 ID，避免改名后命令悬空。
   */
  private wireTemplateIdSync(): void {
    this.templateIdSubscriptions.forEach(s => s.unsubscribe());
    this.templateIdSubscriptions = [];
    const arr = this.protocolTemplatesArray;
    if (!arr) {
      return;
    }
    const single = arr.length <= 1;
    for (let i = 0; i < arr.length; i++) {
      const idCtrl = (arr.at(i) as UntypedFormGroup).get('id');
      if (!idCtrl) {
        continue;
      }
      let previous = String(idCtrl.value ?? '').trim();
      this.templateIdSubscriptions.push(idCtrl.valueChanges.subscribe(v => {
        const next = String(v ?? '').trim();
        if (!next || next === previous) {
          return;
        }
        if (single) {
          this.assignCommandTemplateId(() => true, next);
        } else if (previous) {
          this.assignCommandTemplateId(tid => tid === previous, next);
        }
        previous = next;
      }));
    }
  }

  private assignCommandTemplateId(match: (tid: string) => boolean, value: string): void {
    for (let c = 0; c < this.protocolCommandsArray.length; c++) {
      const ctl = (this.protocolCommandsArray.at(c) as UntypedFormGroup).get('templateId');
      if (!ctl) {
        continue;
      }
      if (match(String(ctl.value ?? '').trim())) {
        ctl.patchValue(value, { emitEvent: false });
      }
    }
  }

  /** 与帧模板「固定线值整型」一致：从模型载入时用十进制回显，避免把 201 显示成 0xc9。 */
  private formatCommandValueForForm(v: number | undefined | null): string {
    const s = formatFixedWireIntegralFromModel(v);
    return s === '' ? '0' : s;
  }

  private syncCommandTemplateIdsFromFirstTemplate(): void {
    const idCtrl = this.protocolTemplatesArray?.at(0)?.get('id');
    const tid = idCtrl ? String(idCtrl.value ?? '').trim() : '';
    if (!tid) {
      return;
    }
    // 多模板时只给「还没归属」的命令补默认值，不覆盖已有归属
    const overwriteAll = (this.protocolTemplatesArray?.length ?? 0) <= 1;
    for (let i = 0; i < this.protocolCommandsArray.length; i++) {
      const ctl = (this.protocolCommandsArray.at(i) as UntypedFormGroup).get('templateId');
      if (!ctl) {
        continue;
      }
      if (overwriteAll || !String(ctl.value ?? '').trim()) {
        ctl.patchValue(tid, { emitEvent: false });
      }
    }
  }
}
