///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { UntypedFormArray, UntypedFormGroup } from '@angular/forms';
import {
  ProtocolTemplateCommandDirection,
  TcpHexLtvChunkOrder,
  TcpHexUnknownTagMode,
  TcpHexValueType,
  TransportTcpDataType
} from '@shared/models/device.models';

@Component({
  selector: 'tb-protocol-template-tcp-data-configuration',
  templateUrl: './protocol-template-tcp-data-configuration.component.html',
  styleUrls: ['./protocol-template-tcp-data-configuration.component.scss']
})
export class ProtocolTemplateTcpDataConfigurationComponent {
  readonly TransportTcpDataType = TransportTcpDataType;
  readonly ProtocolTemplateCommandDirection = ProtocolTemplateCommandDirection;
  readonly TcpHexValueType = TcpHexValueType;
  readonly TcpHexLtvChunkOrder = TcpHexLtvChunkOrder;
  readonly TcpHexUnknownTagMode = TcpHexUnknownTagMode;
  readonly tcpHexMatchValueTypes = [
    TcpHexValueType.UINT8,
    TcpHexValueType.INT8,
    TcpHexValueType.UINT16_BE,
    TcpHexValueType.UINT16_LE,
    TcpHexValueType.INT16_BE,
    TcpHexValueType.INT16_LE,
    TcpHexValueType.UINT32_BE,
    TcpHexValueType.UINT32_LE,
    TcpHexValueType.INT32_BE,
    TcpHexValueType.INT32_LE
  ];

  @Input()
  templatesArray: UntypedFormArray;
  @Input()
  commandsArray: UntypedFormArray;
  @Input()
  tcpHexValueTypes: TcpHexValueType[];
  @Input()
  disabled: boolean;

  /** full：模板+命令同页；分段用于对话框 Tab */
  @Input()
  layoutMode: 'full' | 'templatesOnly' | 'commandsOnly' = 'full';

  /** 仅允许一个帧模板（协议模板库与监控协议负载均使用） */
  @Input()
  singleTemplateMode = true;

  @Output() addTemplateField = new EventEmitter<number>();
  @Output() removeTemplateField = new EventEmitter<{ templateIndex: number; fieldIndex: number }>();
  @Output() addTemplateLtvMapping = new EventEmitter<number>();
  @Output() removeTemplateLtvMapping = new EventEmitter<{ templateIndex: number; index: number }>();
  @Output() addCommand = new EventEmitter<void>();
  @Output() removeCommand = new EventEmitter<number>();
  @Output() addOverrideField = new EventEmitter<number>();
  @Output() removeOverrideField = new EventEmitter<{ commandIndex: number; fieldIndex: number }>();

  asFormGroup(ctrl: unknown): UntypedFormGroup {
    return ctrl as UntypedFormGroup;
  }

  getTemplateFieldsArray(ti: number): UntypedFormArray {
    return (this.templatesArray.at(ti) as UntypedFormGroup).get('hexProtocolFields') as UntypedFormArray;
  }

  getTemplateLtvTagMappingsArray(ti: number): UntypedFormArray {
    return (this.templatesArray.at(ti) as UntypedFormGroup).get('hexLtvTagMappings') as UntypedFormArray;
  }

  getCommandOverrideFieldsArray(ci: number): UntypedFormArray {
    return (this.commandsArray.at(ci) as UntypedFormGroup).get('overrideFields') as UntypedFormArray;
  }

  isIntegralHexValueType(vt: TcpHexValueType | null | undefined): boolean {
    if (vt == null) {
      return false;
    }
    return this.tcpHexMatchValueTypes.includes(vt);
  }

  isIntegralHexFieldTemplateRow(fi: number): boolean {
    const g = this.getTemplateFieldsArray(0).at(fi) as UntypedFormGroup;
    return this.isIntegralHexValueType(g.get('valueType')?.value as TcpHexValueType);
  }

  isIntegralHexFieldOverrideRow(ci: number, fi: number): boolean {
    const g = this.getCommandOverrideFieldsArray(ci).at(fi) as UntypedFormGroup;
    return this.isIntegralHexValueType(g.get('valueType')?.value as TcpHexValueType);
  }

  isBytesAsHexTemplate(ti: number, fi: number): boolean {
    const g = this.getTemplateFieldsArray(ti).at(fi) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  isBytesAsHexOverride(ci: number, fi: number): boolean {
    const g = this.getCommandOverrideFieldsArray(ci).at(fi) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  isBytesAsHexLtvTemplateTag(ti: number, li: number): boolean {
    const g = this.getTemplateLtvTagMappingsArray(ti).at(li) as UntypedFormGroup;
    return g.get('valueType')?.value === TcpHexValueType.BYTES_AS_HEX;
  }

  firstTemplateId(): string {
    if (!this.templatesArray?.length) {
      return '';
    }
    const id = (this.templatesArray.at(0) as UntypedFormGroup).get('id')?.value;
    return id ? String(id).trim() : '';
  }

  templateIdOptions(): string[] {
    if (!this.templatesArray?.length) {
      return [];
    }
    const ids: string[] = [];
    for (let i = 0; i < this.templatesArray.length; i++) {
      const id = (this.templatesArray.at(i) as UntypedFormGroup).get('id')?.value;
      if (id && String(id).trim()) {
        ids.push(String(id).trim());
      }
    }
    return ids;
  }

  /** 「上下行命令」Tab 中与首帧模板共享命令偏移/宽度，避免与帧模板 Tab 重复嵌套 [formGroup] */
  firstTemplateGroup(): UntypedFormGroup {
    return this.templatesArray.at(0) as UntypedFormGroup;
  }

  getHexFieldLengthModeTemplate(ti: number, fi: number): 'fixed' | 'fromFrame' {
    return this.hexFieldLengthModeFromGroup(this.getTemplateFieldsArray(ti).at(fi) as UntypedFormGroup);
  }

  getHexFieldLengthModeOverride(ci: number, fi: number): 'fixed' | 'fromFrame' {
    return this.hexFieldLengthModeFromGroup(this.getCommandOverrideFieldsArray(ci).at(fi) as UntypedFormGroup);
  }

  private hexFieldLengthModeFromGroup(g: UntypedFormGroup): 'fixed' | 'fromFrame' {
    const m = g.get('hexFieldLengthMode')?.value;
    if (m === 'fromFrame' || m === 'fixed') {
      return m;
    }
    const fromOff = g.get('byteLengthFromByteOffset')?.value;
    return fromOff !== null && fromOff !== undefined && fromOff !== '' ? 'fromFrame' : 'fixed';
  }

  onHexFieldLengthModeTemplate(ti: number, fi: number, mode: 'fixed' | 'fromFrame'): void {
    this.applyHexFieldLengthMode(this.getTemplateFieldsArray(ti).at(fi) as UntypedFormGroup, mode);
  }

  onHexFieldLengthModeOverride(ci: number, fi: number, mode: 'fixed' | 'fromFrame'): void {
    this.applyHexFieldLengthMode(this.getCommandOverrideFieldsArray(ci).at(fi) as UntypedFormGroup, mode);
  }

  private applyHexFieldLengthMode(g: UntypedFormGroup, mode: 'fixed' | 'fromFrame'): void {
    g.get('hexFieldLengthMode')?.patchValue(mode, { emitEvent: false });
    if (mode === 'fixed') {
      g.patchValue({ byteLengthFromByteOffset: null, byteLengthFromValueType: TcpHexValueType.UINT8 }, { emitEvent: false });
    } else {
      g.patchValue({ byteLength: null }, { emitEvent: false });
    }
  }

  isDownlinkOrBothForCommand(ci: number): boolean {
    const g = this.commandsArray?.at(ci) as UntypedFormGroup;
    const d = g?.get('direction')?.value;
    return d === ProtocolTemplateCommandDirection.DOWNLINK || d === ProtocolTemplateCommandDirection.BOTH;
  }

  /** 可选作「长度字段」的整型 key：首帧模板 + 本命令覆盖行 */
  downlinkLengthFieldKeyOptions(ci: number): string[] {
    const keys = new Set<string>();
    const addIntegral = (fg: UntypedFormGroup) => {
      const k = String(fg.get('key')?.value ?? '').trim();
      const vt = fg.get('valueType')?.value as TcpHexValueType;
      if (k && this.tcpHexMatchValueTypes.includes(vt)) {
        keys.add(k);
      }
    };
    if (this.templatesArray?.length) {
      const fa = this.getTemplateFieldsArray(0);
      for (let i = 0; i < fa.length; i++) {
        addIntegral(fa.at(i) as UntypedFormGroup);
      }
    }
    const oa = this.getCommandOverrideFieldsArray(ci);
    for (let i = 0; i < oa.length; i++) {
      addIntegral(oa.at(i) as UntypedFormGroup);
    }
    return Array.from(keys).sort();
  }
}
