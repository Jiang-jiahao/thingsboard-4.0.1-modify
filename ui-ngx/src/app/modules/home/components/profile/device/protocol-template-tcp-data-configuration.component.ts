///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { ChangeDetectorRef, Component, EventEmitter, Input, AfterViewInit, OnChanges, OnDestroy, Output, SimpleChanges } from '@angular/core';
import { AbstractControl, UntypedFormArray, UntypedFormGroup } from '@angular/forms';
import {
  ProtocolTemplateCommandDirection,
  TcpHexLtvChunkOrder,
  TcpHexUnknownTagMode,
  TcpHexValueType,
  TCP_HEX_FRAME_FIELD_VALUE_TYPES,
  TCP_HEX_LTV_TAG_VALUE_OPTIONS,
  TcpHexLtvTagValueOption,
  TransportTcpDataType
} from '@shared/models/device.models';
import {
  formatIntegralWireTextEcho,
  formatTcpHexMatchValueHexHint,
  parseIntegralWireTextToNumber,
  parseLtvTagWireTextToNumber
} from '@home/pages/profiles/protocol-template-downlink-fields.util';
import { Subject, Subscription } from 'rxjs';
import { startWith, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-protocol-template-tcp-data-configuration',
  templateUrl: './protocol-template-tcp-data-configuration.component.html',
  styleUrls: ['./protocol-template-tcp-data-configuration.component.scss']
})
export class ProtocolTemplateTcpDataConfigurationComponent implements OnChanges, AfterViewInit, OnDestroy {
  /** 帧模板内 LTV/TLV 段折叠面板：已启用 LTV 时打开界面默认展开 */
  templateHexLtvPanelExpanded = false;

  private readonly destroy$ = new Subject<void>();
  private templatesArraySub?: Subscription;
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
  tcpHexValueTypes: TcpHexValueType[] = TCP_HEX_FRAME_FIELD_VALUE_TYPES;
  /** LTV Tag→遥测映射：与帧内固定字段的 tcpHexValueTypes 分离 */
  @Input()
  tcpHexLtvTagValueOptions: TcpHexLtvTagValueOption[] = TCP_HEX_LTV_TAG_VALUE_OPTIONS;
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

  constructor(private cdr: ChangeDetectorRef) {
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['templatesArray']) {
      this.wireTemplateLtvPanelSync();
      this.scheduleExpandTemplateLtvPanel();
    }
  }

  ngAfterViewInit(): void {
    this.wireTemplateLtvPanelSync();
    this.scheduleExpandTemplateLtvPanel();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.templatesArraySub?.unsubscribe();
  }

  /**
   * templatesArray 多为同一 FormArray 引用原地更新，ngOnChanges 不一定触发；
   * 父级 patch 常带 emitEvent:false，valueChanges 也可能不触发——需订阅 valueChanges（含 startWith）并延迟再读当前值。
   */
  private wireTemplateLtvPanelSync(): void {
    this.templatesArraySub?.unsubscribe();
    if (!this.templatesArray) {
      return;
    }
    this.templatesArraySub = this.templatesArray.valueChanges.pipe(
      startWith(this.templatesArray.value),
      takeUntil(this.destroy$)
    ).subscribe(() => this.expandTemplateLtvPanelIfEnabled());
  }

  private scheduleExpandTemplateLtvPanel(): void {
    const run = () => this.expandTemplateLtvPanelIfEnabled();
    queueMicrotask(run);
    setTimeout(run, 0);
    setTimeout(run, 50);
  }

  private expandTemplateLtvPanelIfEnabled(): void {
    if (!this.templatesArray?.length) {
      return;
    }
    const g = this.templatesArray.at(0) as UntypedFormGroup;
    if (g?.get('hexLtvEnabled')?.value === true) {
      this.templateHexLtvPanelExpanded = true;
      this.cdr.markForCheck();
    }
  }

  onTemplateHexLtvEnabledChange(checked: boolean): void {
    if (checked) {
      this.templateHexLtvPanelExpanded = true;
      this.cdr.markForCheck();
    }
  }

  asFormGroup(ctrl: unknown): UntypedFormGroup {
    return ctrl as UntypedFormGroup;
  }

  getTemplateFieldsArray(ti: number): UntypedFormArray {
    return (this.templatesArray.at(ti) as UntypedFormGroup).get('hexProtocolFields') as UntypedFormArray;
  }

  /** 列表按字节偏移升序展示；下标仍为 FormArray 真实索引，便于绑定与删除 */
  templateFieldIndicesSortedByByteOffset(ti: number): number[] {
    return this.sortedHexFieldIndicesByByteOffset(this.getTemplateFieldsArray(ti));
  }

  overrideFieldIndicesSortedByByteOffset(ci: number): number[] {
    return this.sortedHexFieldIndicesByByteOffset(this.getCommandOverrideFieldsArray(ci));
  }

  private sortedHexFieldIndicesByByteOffset(fa: UntypedFormArray): number[] {
    const n = fa.length;
    return Array.from({ length: n }, (_, i) => i).sort((a, b) => {
      const da = this.byteOffsetAtFormIndex(fa, a);
      const db = this.byteOffsetAtFormIndex(fa, b);
      if (da !== db) {
        return da - db;
      }
      return a - b;
    });
  }

  private byteOffsetAtFormIndex(fa: UntypedFormArray, i: number): number {
    const v = (fa.at(i) as UntypedFormGroup).get('byteOffset')?.value;
    const num = Number(v);
    return Number.isFinite(num) ? num : Number.MAX_SAFE_INTEGER;
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

  onLtvTagValueBlur(ti: number, li: number): void {
    if (this.disabled) {
      return;
    }
    const tpl = this.templatesArray.at(ti) as UntypedFormGroup;
    const vt = (tpl.get('hexLtvTagType')?.value ?? TcpHexValueType.UINT8) as TcpHexValueType;
    const g = this.getTemplateLtvTagMappingsArray(ti).at(li) as UntypedFormGroup;
    const ctrl = g.get('tagValue');
    const t = String(ctrl?.value ?? '');
    if (!t.trim()) {
      return;
    }
    const n = parseLtvTagWireTextToNumber(t);
    if (n === undefined) {
      return;
    }
    ctrl?.patchValue(formatTcpHexMatchValueHexHint(n, vt), { emitEvent: false });
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
  /** 固定线值：失焦后按 0x→十六进制、否则十进制归一化回显 */
  onFixedWireIntegralBlur(ctrl: AbstractControl | null): void {
    if (!ctrl || this.disabled) {
      return;
    }
    const t = String(ctrl.value ?? '');
    if (!t.trim()) {
      return;
    }
    const n = parseIntegralWireTextToNumber(t);
    if (n === undefined) {
      return;
    }
    ctrl.patchValue(formatIntegralWireTextEcho(t, n), { emitEvent: false });
  }

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
