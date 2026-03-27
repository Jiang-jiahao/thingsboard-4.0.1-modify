///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
///

import { Component } from '@angular/core';
import { AbstractControl, FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { RuleNodeConfiguration, RuleNodeConfigurationComponent } from '@shared/models/rule-node.models';
import { HEX_PARSER_DEFAULT_PROTOCOL_EXAMPLE } from './hex-protocol-parser-example.config';

export const HEX_FIELD_TYPES = [
  'UINT8',
  'UINT16_LE',
  'UINT16_BE',
  'UINT32_LE',
  'UINT32_BE',
  'FLOAT32_LE',
  'FLOAT32_BE',
  'FLOAT64_LE',
  'FLOAT64_BE',
  'HEX_SLICE',
  'HEX_SLICE_LEN_U16LE',
  'BOOL_BIT',
  'LIST',
  'UNIT_LIST',
  'STRUCT',
  'GENERIC_LIST'
] as const;

export const HEX_CHECKSUM_TYPES = ['NONE', 'SUM8', 'CRC16_MODBUS', 'CRC16_CCITT', 'CRC32'] as const;

export const HEX_GENERIC_LIST_COUNT_MODES = ['UNTIL_END', 'FIXED', 'FROM_FIELD'] as const;
export const HEX_GENERIC_LIST_LENGTH_MODES = [
  'FIXED',
  'PREFIX_UINT8',
  'PREFIX_UINT16_LE',
  'PREFIX_UINT16_BE',
  'PREFIX_UINT32_LE'
] as const;
export const HEX_COUNT_FIELD_TYPES = ['UINT8', 'UINT16_LE', 'UINT16_BE', 'UINT32_LE', 'UINT32_BE'] as const;

@Component({
  selector: 'tb-transformation-node-hex-protocol-parser-config',
  templateUrl: './hex-protocol-parser-config.component.html',
  styleUrls: ['./hex-protocol-parser-config.component.scss']
})
export class HexProtocolParserConfigComponent extends RuleNodeConfigurationComponent {

  hexProtocolParserConfigForm: FormGroup;
  fieldTypes = [...HEX_FIELD_TYPES];
  checksumTypes = [...HEX_CHECKSUM_TYPES];
  genericListCountModes = [...HEX_GENERIC_LIST_COUNT_MODES];
  genericListLengthModes = [...HEX_GENERIC_LIST_LENGTH_MODES];
  countFieldTypes = [...HEX_COUNT_FIELD_TYPES];
  showGeneratedJson = false;

  /** 与后端默认配置一致，供对照或复制 */
  get hexDefaultProtocolExampleJson(): string {
    return JSON.stringify(HEX_PARSER_DEFAULT_PROTOCOL_EXAMPLE, null, 2);
  }

  constructor(private fb: FormBuilder) {
    super();
  }

  protected configForm(): FormGroup {
    return this.hexProtocolParserConfigForm;
  }

  get frameTemplatesFormArray(): FormArray {
    return this.hexProtocolParserConfigForm.get('frameTemplates') as FormArray;
  }

  get protocolsFormArray(): FormArray {
    return this.hexProtocolParserConfigForm.get('protocols') as FormArray;
  }

  fieldsFormArray(pi: number): FormArray {
    return this.protocolsFormArray.at(pi).get('fields') as FormArray;
  }

  /** Custom header fields inside a frame template (frame header layout) */
  templateHeaderFieldsFormArray(ti: number): FormArray {
    return this.frameTemplatesFormArray.at(ti).get('headerFields') as FormArray;
  }

  /** 帧模板：子命令体 / 参数区（与协议中的命令匹配配合；偏移相对 paramStartOffset） */
  templatePayloadFieldsFormArray(ti: number): FormArray {
    return this.frameTemplatesFormArray.at(ti).get('payloadFields') as FormArray;
  }

  protected onConfigurationSet(configuration: RuleNodeConfiguration) {
    this.hexProtocolParserConfigForm = this.fb.group({
      hexInputKey: [configuration?.hexInputKey ?? 'rawHex', [Validators.required]],
      protocolIdKey: [configuration?.protocolIdKey ?? ''],
      resultObjectKey: [configuration?.resultObjectKey ?? 'parsed'],
      outputKeyPrefix: [configuration?.outputKeyPrefix ?? ''],
      frameTemplates: this.fb.array([]),
      protocols: this.fb.array([])
    });
    this.replaceFrameTemplatesFromConfig(configuration?.frameTemplates);
    this.replaceProtocolsFromConfig(configuration?.protocols);
  }

  protected updateConfiguration(configuration: RuleNodeConfiguration) {
    const c = configuration || {} as RuleNodeConfiguration;
    this.hexProtocolParserConfigForm.patchValue({
      hexInputKey: c.hexInputKey ?? 'rawHex',
      protocolIdKey: c.protocolIdKey ?? '',
      resultObjectKey: c.resultObjectKey ?? 'parsed',
      outputKeyPrefix: c.outputKeyPrefix ?? ''
    }, {emitEvent: false});
    this.replaceFrameTemplatesFromConfig(c.frameTemplates);
    this.replaceProtocolsFromConfig(c.protocols);
    this.updateValidators(false);
  }

  private replaceFrameTemplatesFromConfig(templates: any[]) {
    const arr = this.frameTemplatesFormArray;
    while (arr.length) {
      arr.removeAt(0);
    }
    if (templates?.length) {
      for (const t of templates) {
        arr.push(this.createFrameTemplateGroup(t));
      }
    } else {
      arr.push(this.createFrameTemplateGroup());
    }
  }

  private replaceProtocolsFromConfig(protocols: any[]) {
    const arr = this.protocolsFormArray;
    while (arr.length) {
      arr.removeAt(0);
    }
    const list = protocols?.length ? protocols : [null];
    for (const p of list) {
      arr.push(this.createProtocolGroup(p));
    }
  }

  private createFrameTemplateGroup(t?: any): FormGroup {
    const cs = t?.checksum;
    return this.fb.group({
      id: [t?.id ?? ''],
      syncHex: [t?.syncHex ?? ''],
      syncOffset: [t?.syncOffset ?? 0],
      minBytes: [t?.minBytes ?? 0],
      commandMatchOffset: [t?.commandMatchOffset ?? ''],
      paramLenFieldOffset: [t?.paramLenFieldOffset ?? 5],
      paramStartOffset: [t?.paramStartOffset ?? 7],
      useChecksum: [!!cs],
      checksumType: [cs?.type ?? 'SUM8'],
      checksumFromByte: [cs?.fromByte ?? 0],
      checksumToExclusive: [cs?.toExclusive ?? -1],
      checksumByteIndex: [cs?.checksumByteIndex ?? -1],
      headerFields: this.fb.array(
        (t?.headerFields?.length ? t.headerFields : []).map((f: any) => this.createFieldGroup(f))
      ),
      payloadFields: this.fb.array(
        (t?.payloadFields?.length ? t.payloadFields : []).map((f: any) => this.createFieldGroup(f))
      )
    });
  }

  private createProtocolGroup(p?: any): FormGroup {
    const cs = p?.checksum;
    const fieldRows = p?.fields?.length ? p.fields : (p?.templateId ? [] : [null]);
    return this.fb.group({
      id: [p?.id ?? '', [Validators.required]],
      templateId: [p?.templateId ?? ''],
      payloadOffsetsRelative: [p?.payloadOffsetsRelative !== false],
      syncHex: [p?.syncHex ?? ''],
      syncOffset: [p?.syncOffset ?? 0],
      minBytes: [p?.minBytes ?? 0],
      commandByteOffset: [p?.commandByteOffset ?? ''],
      commandValue: [p?.commandValue ?? ''],
      commandMatchWidth: [p?.commandMatchWidth === 4 ? 4 : 1],
      useChecksum: [!!cs],
      checksumType: [cs?.type ?? 'SUM8'],
      checksumFromByte: [cs?.fromByte ?? 0],
      checksumToExclusive: [cs?.toExclusive ?? -1],
      checksumByteIndex: [cs?.checksumByteIndex ?? -1],
      fields: this.fb.array(
        fieldRows.map((f: any) => this.createFieldGroup(f))
      )
    });
  }

  /** 后端仍用 TLV_LIST；表单里用 LIST，避免界面出现技术缩写 */
  private normalizeFieldTypeForForm(t?: string): string {
    if (t === 'TLV_LIST') {
      return 'LIST';
    }
    return t ?? 'UINT8';
  }

  private listItemFieldsFromConfig(f: any): any[] {
    if (f?.listItemFields?.length) {
      return f.listItemFields;
    }
    if (f?.tlvNestedRules?.length) {
      const first = f.tlvNestedRules.find((r: any) => r.fields?.length);
      return first?.fields ?? [];
    }
    return [];
  }

  private createFieldGroup(f?: any): FormGroup {
    return this.fb.group({
      name: [f?.name ?? '', [Validators.required]],
      offset: [f?.offset ?? 0, [Validators.required]],
      type: [this.normalizeFieldTypeForForm(f?.type), [Validators.required]],
      length: [f?.length ?? ''],
      bitIndex: [f?.bitIndex ?? ''],
      lengthFieldOffset: [f?.lengthFieldOffset ?? 5],
      toOffsetExclusive: [f?.toOffsetExclusive ?? ''],
      unitLengthFieldOffset: [f?.unitLengthFieldOffset ?? 0],
      unitIdByteLength: [f?.unitIdByteLength ?? 4],
      unitDataOutputName: [f?.unitDataOutputName ?? ''],
      unitNumberOutputName: [f?.unitNumberOutputName ?? ''],
      listCountMode: [f?.listCountMode ?? 'UNTIL_END'],
      listCount: [f?.listCount ?? ''],
      listCountFieldOffset: [f?.listCountFieldOffset ?? ''],
      listCountFieldType: [f?.listCountFieldType ?? 'UINT8'],
      listItemLengthMode: [f?.listItemLengthMode ?? 'PREFIX_UINT8'],
      listItemFixedLength: [f?.listItemFixedLength ?? ''],
      listItemFields: this.fb.array(
        this.listItemFieldsFromConfig(f).map((x: any) => this.createFieldGroup(x))
      ),
      nestedFields: this.fb.array(
        (f?.nestedFields?.length ? f.nestedFields : []).map((x: any) => this.createFieldGroup(x))
      )
    });
  }

  listItemFieldsProtocol(pi: number, fi: number): FormArray {
    return this.fieldsFormArray(pi).at(fi).get('listItemFields') as FormArray;
  }

  listItemFieldsTemplate(ti: number, fi: number): FormArray {
    return this.templateHeaderFieldsFormArray(ti).at(fi).get('listItemFields') as FormArray;
  }

  listItemFieldsTemplatePayload(ti: number, fi: number): FormArray {
    return this.templatePayloadFieldsFormArray(ti).at(fi).get('listItemFields') as FormArray;
  }

  nestedFieldsProtocol(pi: number, fi: number): FormArray {
    return this.fieldsFormArray(pi).at(fi).get('nestedFields') as FormArray;
  }

  nestedFieldsTemplate(ti: number, fi: number): FormArray {
    return this.templateHeaderFieldsFormArray(ti).at(fi).get('nestedFields') as FormArray;
  }

  nestedFieldsTemplatePayload(ti: number, fi: number): FormArray {
    return this.templatePayloadFieldsFormArray(ti).at(fi).get('nestedFields') as FormArray;
  }

  addNestedFieldProtocol(pi: number, fi: number) {
    this.nestedFieldsProtocol(pi, fi).push(this.createFieldGroup());
  }

  addNestedFieldTemplate(ti: number, fi: number) {
    this.nestedFieldsTemplate(ti, fi).push(this.createFieldGroup());
  }

  addNestedFieldTemplatePayload(ti: number, fi: number) {
    this.nestedFieldsTemplatePayload(ti, fi).push(this.createFieldGroup());
  }

  addListItemFieldProtocol(pi: number, fi: number) {
    this.listItemFieldsProtocol(pi, fi).push(this.createFieldGroup());
  }

  removeListItemFieldProtocol(pi: number, fi: number, li: number) {
    this.listItemFieldsProtocol(pi, fi).removeAt(li);
  }

  addListItemFieldTemplate(ti: number, fi: number) {
    this.listItemFieldsTemplate(ti, fi).push(this.createFieldGroup());
  }

  addListItemFieldTemplatePayload(ti: number, fi: number) {
    this.listItemFieldsTemplatePayload(ti, fi).push(this.createFieldGroup());
  }

  removeListItemFieldTemplate(ti: number, fi: number, li: number) {
    this.listItemFieldsTemplate(ti, fi).removeAt(li);
  }

  removeListItemFieldTemplatePayload(ti: number, fi: number, li: number) {
    this.listItemFieldsTemplatePayload(ti, fi).removeAt(li);
  }

  addTemplatePayloadField(ti: number) {
    this.templatePayloadFieldsFormArray(ti).push(this.createFieldGroup());
  }

  removeTemplatePayloadField(ti: number, fi: number) {
    const fa = this.templatePayloadFieldsFormArray(ti);
    if (fa.length > 1) {
      fa.removeAt(fi);
    } else if (fa.length === 1) {
      fa.removeAt(0);
    }
  }

  /** 递归列表子字段：任意深度嵌套「列表」 */
  asFormArray(c: AbstractControl | null | undefined): FormArray {
    return c as FormArray;
  }

  /** 与 FormArray 子项用 [formGroup] 绑定，避免 ng-template/ngTemplateOutlet 下 formGroupName 索引解析失败 */
  asFormGroup(c: AbstractControl | null | undefined): FormGroup {
    return c as FormGroup;
  }

  addListItemToArray(fa: FormArray | null | undefined): void {
    if (!fa) {
      return;
    }
    fa.push(this.createFieldGroup());
  }

  removeListItemFromArray(fa: FormArray, idx: number): void {
    fa.removeAt(idx);
  }

  fieldTypeIsListOrUnitList(t: string | null | undefined): boolean {
    return t === 'LIST' || t === 'UNIT_LIST';
  }

  /** LIST / UNIT_LIST / GENERIC_LIST：区间结束 + 子字段（及通用列表的计数/长度模式） */
  fieldTypeShowsRepeatBlock(t: string | null | undefined): boolean {
    return t === 'LIST' || t === 'UNIT_LIST' || t === 'GENERIC_LIST';
  }

  fieldTypeIsStruct(t: string | null | undefined): boolean {
    return t === 'STRUCT';
  }

  addNestedFieldToArray(fa: FormArray | null | undefined): void {
    if (!fa) {
      return;
    }
    fa.push(this.createFieldGroup());
  }

  addFrameTemplate() {
    this.frameTemplatesFormArray.push(this.createFrameTemplateGroup());
  }

  removeFrameTemplate(index: number) {
    if (this.frameTemplatesFormArray.length > 1) {
      this.frameTemplatesFormArray.removeAt(index);
    }
  }

  addTemplateHeaderField(templateIndex: number) {
    this.templateHeaderFieldsFormArray(templateIndex).push(this.createFieldGroup());
  }

  removeTemplateHeaderField(templateIndex: number, fieldIndex: number) {
    const fa = this.templateHeaderFieldsFormArray(templateIndex);
    if (fa.length > 1) {
      fa.removeAt(fieldIndex);
    } else if (fa.length === 1) {
      fa.removeAt(0);
    }
  }

  addProtocol() {
    this.protocolsFormArray.push(this.createProtocolGroup());
  }

  removeProtocol(index: number) {
    if (this.protocolsFormArray.length > 1) {
      this.protocolsFormArray.removeAt(index);
    }
  }

  addField(protocolIndex: number) {
    this.fieldsFormArray(protocolIndex).push(this.createFieldGroup());
  }

  removeField(protocolIndex: number, fieldIndex: number) {
    const fa = this.fieldsFormArray(protocolIndex);
    const hasTemplate = !!this.protocolsFormArray.at(protocolIndex).get('templateId')?.value;
    if (fa.length > 1 || (hasTemplate && fa.length > 0)) {
      fa.removeAt(fieldIndex);
    }
  }

  protected prepareOutputConfig(configuration: RuleNodeConfiguration): RuleNodeConfiguration {
    const v = configuration as any;
    return {
      hexInputKey: v.hexInputKey,
      protocolIdKey: v.protocolIdKey,
      resultObjectKey: v.resultObjectKey,
      outputKeyPrefix: v.outputKeyPrefix,
      frameTemplates: this.serializeFrameTemplates(v.frameTemplates),
      protocols: this.serializeProtocols(v.protocols)
    };
  }

  private serializeFrameTemplates(rows: any[]): any[] {
    return (rows || []).filter(t => (t.id || '').trim()).map(t => {
      const o: any = {
        id: (t.id || '').trim(),
        syncOffset: this.num(t.syncOffset, 0),
        minBytes: this.num(t.minBytes, 0),
        paramLenFieldOffset: this.num(t.paramLenFieldOffset, 5),
        paramStartOffset: this.num(t.paramStartOffset, 7)
      };
      const cmo = t.commandMatchOffset;
      if (cmo !== '' && cmo != null && !isNaN(Number(cmo))) {
        o.commandMatchOffset = Number(cmo);
      }
      const sh = (t.syncHex || '').replace(/\s/g, '');
      if (sh) {
        o.syncHex = sh.toUpperCase();
      }
      if (t.useChecksum && t.checksumType && t.checksumType !== 'NONE') {
        o.checksum = {
          type: t.checksumType,
          fromByte: this.num(t.checksumFromByte, 0),
          toExclusive: this.num(t.checksumToExclusive, -1),
          checksumByteIndex: this.num(t.checksumByteIndex, -1)
        };
      }
      const headerFields = (t.headerFields || [])
        .filter((f: any) => (f.name || '').toString().trim())
        .map((f: any) => this.serializeFieldDefinition(f));
      if (headerFields.length) {
        o.headerFields = headerFields;
      }
      const payloadFields = (t.payloadFields || [])
        .filter((f: any) => (f.name || '').toString().trim())
        .map((f: any) => this.serializeFieldDefinition(f));
      if (payloadFields.length) {
        o.payloadFields = payloadFields;
      }
      return o;
    });
  }

  private serializeProtocols(rows: any[]): any[] {
    return (rows || []).map(p => {
      const proto: any = {
        id: (p.id || '').trim(),
        syncOffset: this.num(p.syncOffset, 0),
        minBytes: this.num(p.minBytes, 0)
      };
      const tid = (p.templateId || '').trim();
      if (tid) {
        proto.templateId = tid;
        if (p.payloadOffsetsRelative === false) {
          proto.payloadOffsetsRelative = false;
        }
      }
      const sh = (p.syncHex || '').replace(/\s/g, '');
      if (sh && !tid) {
        proto.syncHex = sh.toUpperCase();
      }
      const cmdOff = p.commandByteOffset;
      const cmdVal = p.commandValue;
      if (cmdOff !== '' && cmdOff != null && !isNaN(Number(cmdOff))
          && cmdVal !== '' && cmdVal != null && !isNaN(Number(cmdVal))) {
        proto.commandByteOffset = Number(cmdOff);
        proto.commandValue = Number(cmdVal);
      }
      if (Number(p.commandMatchWidth) === 4) {
        proto.commandMatchWidth = 4;
      }
      if (p.useChecksum && p.checksumType && p.checksumType !== 'NONE') {
        proto.checksum = {
          type: p.checksumType,
          fromByte: this.num(p.checksumFromByte, 0),
          toExclusive: this.num(p.checksumToExclusive, -1),
          checksumByteIndex: this.num(p.checksumByteIndex, -1)
        };
      }
      proto.fields = (p.fields || []).map((f: any) => this.serializeFieldDefinition(f));
      return proto;
    });
  }

  private serializeFieldDefinition(f: any): any {
    const wireType = f.type === 'LIST' ? 'TLV_LIST' : f.type;
    const fld: any = {
      name: (f.name || '').trim(),
      offset: this.num(f.offset, 0),
      type: wireType
    };
    if (f.type === 'STRUCT') {
      if (f.length !== '' && f.length != null && !isNaN(Number(f.length)) && Number(f.length) > 0) {
        fld.length = Number(f.length);
      }
      if (f.toOffsetExclusive !== '' && f.toOffsetExclusive != null && !isNaN(Number(f.toOffsetExclusive))) {
        fld.toOffsetExclusive = Number(f.toOffsetExclusive);
      }
      const nested = (f.nestedFields || [])
        .filter((nf: any) => (nf.name || '').toString().trim())
        .map((nf: any) => this.serializeFieldDefinition(nf));
      if (nested.length) {
        fld.nestedFields = nested;
      }
      return fld;
    }
    if (f.type === 'HEX_SLICE' && f.length !== '' && f.length != null && !isNaN(Number(f.length))) {
      const n = Number(f.length);
      if (n > 0) {
        fld.length = n;
      }
    }
    if (f.type === 'HEX_SLICE_LEN_U16LE' && f.lengthFieldOffset !== '' && f.lengthFieldOffset != null
        && !isNaN(Number(f.lengthFieldOffset))) {
      fld.lengthFieldOffset = Number(f.lengthFieldOffset);
    }
    if (f.type === 'BOOL_BIT' && f.bitIndex !== '' && f.bitIndex != null && !isNaN(Number(f.bitIndex))) {
      fld.bitIndex = Number(f.bitIndex);
    }
    if (f.type === 'LIST') {
      if (f.toOffsetExclusive !== '' && f.toOffsetExclusive != null && !isNaN(Number(f.toOffsetExclusive))) {
        fld.toOffsetExclusive = Number(f.toOffsetExclusive);
      }
      const items = (f.listItemFields || [])
        .filter((nf: any) => (nf.name || '').toString().trim())
        .map((nf: any) => this.serializeFieldDefinition(nf));
      if (items.length) {
        fld.listItemFields = items;
      }
    }
    if (f.type === 'UNIT_LIST') {
      if (f.toOffsetExclusive !== '' && f.toOffsetExclusive != null && !isNaN(Number(f.toOffsetExclusive))) {
        fld.toOffsetExclusive = Number(f.toOffsetExclusive);
      }
      const items = (f.listItemFields || [])
        .filter((nf: any) => (nf.name || '').toString().trim())
        .map((nf: any) => this.serializeFieldDefinition(nf));
      if (items.length) {
        fld.listItemFields = items;
      }
      const ulo = f.unitLengthFieldOffset;
      if (ulo !== '' && ulo != null && !isNaN(Number(ulo)) && Number(ulo) !== 0) {
        fld.unitLengthFieldOffset = Number(ulo);
      }
      const uib = f.unitIdByteLength;
      if (uib !== '' && uib != null && !isNaN(Number(uib)) && Number(uib) !== 4) {
        fld.unitIdByteLength = Number(uib);
      }
      const udn = (f.unitDataOutputName || '').toString().trim();
      if (udn) {
        fld.unitDataOutputName = udn;
      }
      const uno = (f.unitNumberOutputName || '').toString().trim();
      if (uno) {
        fld.unitNumberOutputName = uno;
      }
    }
    if (f.type === 'GENERIC_LIST') {
      fld.listCountMode = (f.listCountMode || 'UNTIL_END').toString().toUpperCase();
      if (fld.listCountMode === 'FIXED' && f.listCount !== '' && f.listCount != null && !isNaN(Number(f.listCount))) {
        fld.listCount = Number(f.listCount);
      }
      if (fld.listCountMode === 'FROM_FIELD') {
        const cOff = f.listCountFieldOffset;
        if (cOff !== '' && cOff != null && !isNaN(Number(cOff))) {
          fld.listCountFieldOffset = Number(cOff);
        }
        if (f.listCountFieldType) {
          fld.listCountFieldType = f.listCountFieldType;
        }
      }
      fld.listItemLengthMode = (f.listItemLengthMode || 'PREFIX_UINT8').toString().toUpperCase();
      if (fld.listItemLengthMode === 'FIXED' && f.listItemFixedLength !== '' && f.listItemFixedLength != null
          && !isNaN(Number(f.listItemFixedLength))) {
        fld.listItemFixedLength = Number(f.listItemFixedLength);
      }
      if (f.toOffsetExclusive !== '' && f.toOffsetExclusive != null && !isNaN(Number(f.toOffsetExclusive))) {
        fld.toOffsetExclusive = Number(f.toOffsetExclusive);
      }
      const gItems = (f.listItemFields || [])
        .filter((nf: any) => (nf.name || '').toString().trim())
        .map((nf: any) => this.serializeFieldDefinition(nf));
      if (gItems.length) {
        fld.listItemFields = gItems;
      }
    }
    return fld;
  }

  private num(v: any, def: number): number {
    if (v === '' || v == null) {
      return def;
    }
    const n = Number(v);
    return isNaN(n) ? def : n;
  }

  protected validateConfig(): boolean {
    if (!this.hexProtocolParserConfigForm?.valid) {
      return false;
    }
    for (let i = 0; i < this.protocolsFormArray.length; i++) {
      const g = this.protocolsFormArray.at(i) as FormGroup;
      const hasTemplate = !!(g.get('templateId')?.value || '').toString().trim();
      if (!hasTemplate && this.fieldsFormArray(i).length < 1) {
        return false;
      }
    }
    return true;
  }

  get generatedProtocolsJson(): string {
    try {
      const raw = this.hexProtocolParserConfigForm.getRawValue();
      return JSON.stringify({
        frameTemplates: this.serializeFrameTemplates(raw.frameTemplates),
        protocols: this.serializeProtocols(raw.protocols)
      }, null, 2);
    } catch {
      return '{}';
    }
  }

}
