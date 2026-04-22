///
/// Copyright © 2016-2025 The Thingsboard Authors
///
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
  selector: 'tb-protocol-template-bundle-editor',
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

  tcpHexValueTypes = TCP_HEX_FRAME_FIELD_VALUE_TYPES;
  tcpHexLtvTagValueOptions = TCP_HEX_LTV_TAG_VALUE_OPTIONS;

  private templateIdSubscription?: Subscription;

  private readonly ltvTagWireTextValidator = (control: AbstractControl): ValidationErrors | null => {
    const v = parseLtvTagWireTextToNumber(control.value);
    return v === undefined ? { ltvTagInvalid: true } : null;
  };

  constructor(
    private fb: UntypedFormBuilder
  ) {
  }

  ngOnDestroy(): void {
    this.templateIdSubscription?.unsubscribe();
  }

  /**
   * 与对话框顶部「名称」同步：作为唯一展示名，并写入隐藏字段 id，供命令 templateId 关联。
   */
  applyBundleDisplayNameAsTemplateId(displayName: string): void {
    const tid = String(displayName ?? '').trim();
    const g = this.protocolTemplatesArray?.at(0) as UntypedFormGroup;
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
      : this.formatCommandValueForForm(c?.commandValue);
    return this.fb.group({
      templateId: [c?.templateId ?? '', Validators.required],
      name: [c?.name ?? '', [Validators.maxLength(255)]],
      commandValue: [primaryWire, Validators.required],
      matchValueType: [matchVt, Validators.required],
      secondaryMatchByteOffset: [c?.secondaryMatchByteOffset ?? null],
      secondaryMatchValueType: [c?.secondaryMatchValueType ?? TcpHexValueType.UINT8],
      secondaryMatchValue: [
        c?.secondaryMatchValue != null ? this.formatCommandValueForForm(c.secondaryMatchValue) : ''
      ],
      direction: [c?.direction ?? ProtocolTemplateCommandDirection.UPLINK, Validators.required],
      downlinkPayloadLengthAuto: [!!c?.downlinkPayloadLengthAuto],
      downlinkPayloadLengthFieldKey: [c?.downlinkPayloadLengthFieldKey ?? ''],
      overrideFields: overrideArr
    }, { validators: [this.protocolCommandPrimaryWireValidator] });
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

  patchProtocolTemplatesFromModel(templates: ProtocolTemplateDefinition[]) {
    const arr = this.protocolTemplatesArray;
    while (arr.length) {
      arr.removeAt(0, { emitEvent: false });
    }
    const t = templates?.length ? templates[0] : undefined;
    arr.push(this.createProtocolTemplateGroup(t), { emitEvent: false });
    this.wireTemplateIdSync();
    this.syncCommandTemplateIdsFromFirstTemplate();
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
  }

  private wireTemplateIdSync(): void {
    this.templateIdSubscription?.unsubscribe();
    const idCtrl = this.protocolTemplatesArray?.at(0)?.get('id');
    if (!idCtrl) {
      return;
    }
    this.templateIdSubscription = idCtrl.valueChanges.subscribe(v => {
      const tid = String(v ?? '').trim();
      if (!tid) {
        return;
      }
      for (let i = 0; i < this.protocolCommandsArray.length; i++) {
        (this.protocolCommandsArray.at(i) as UntypedFormGroup).get('templateId')?.patchValue(tid, { emitEvent: false });
      }
    });
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
    for (let i = 0; i < this.protocolCommandsArray.length; i++) {
      (this.protocolCommandsArray.at(i) as UntypedFormGroup).get('templateId')?.patchValue(tid, { emitEvent: false });
    }
  }
}
