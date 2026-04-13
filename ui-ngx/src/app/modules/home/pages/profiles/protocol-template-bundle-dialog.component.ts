///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { AfterViewInit, Component, Inject, QueryList, ViewChildren } from '@angular/core';
import { UntypedFormArray, UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { ActionNotificationShow } from '@core/notification/notification.actions';
import { ProtocolTemplateBundleService } from '@core/services/protocol-template-bundle.service';
import { TranslateService } from '@ngx-translate/core';
import { deepClone } from '@core/utils';
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  ProtocolTemplateDefinition,
  TcpHexChecksumDefinition,
  TcpHexFieldDefinition,
  TcpHexLtvChunkOrder,
  TcpHexLtvRepeatingConfig,
  TcpHexLtvTagMapping,
  TcpHexUnknownTagMode,
  TcpHexValueType
} from '@shared/models/device.models';
import { ProtocolTemplateBundleEditorComponent } from '@home/components/profile/device/protocol-template-bundle-editor.component';
import { buildHuanuoJ3000PresetBundle } from '@home/pages/profiles/protocol-template-huanuo-j3000.preset';
import { parseIntegralWireTextToNumber } from '@home/pages/profiles/protocol-template-downlink-fields.util';

export interface ProtocolTemplateBundleDialogData {
  bundle: ProtocolTemplateBundle | null;
  isNew: boolean;
}

@Component({
  selector: 'tb-protocol-template-bundle-dialog',
  templateUrl: './protocol-template-bundle-dialog.component.html',
  styleUrls: ['./protocol-template-bundle-dialog.component.scss']
})
export class ProtocolTemplateBundleDialogComponent implements AfterViewInit {

  /** 对话框内「帧模板 / 命令」各有一个编辑器实例，共享同一 bundleContentForm；序列化必须读父表单，不能依赖某一个子组件引用。 */
  @ViewChildren(ProtocolTemplateBundleEditorComponent)
  private bundleEditorRefs!: QueryList<ProtocolTemplateBundleEditorComponent>;

  readonly isNew: boolean;
  bundleId: string;

  bundleMetaForm: UntypedFormGroup;
  bundleContentForm: UntypedFormGroup;

  constructor(
    private fb: UntypedFormBuilder,
    private dialogRef: MatDialogRef<ProtocolTemplateBundleDialogComponent, ProtocolTemplateBundle | undefined>,
    @Inject(MAT_DIALOG_DATA) public data: ProtocolTemplateBundleDialogData,
    private bundleService: ProtocolTemplateBundleService,
    private store: Store<AppState>,
    private translate: TranslateService
  ) {
    this.isNew = data.isNew;
    this.bundleId = data.bundle?.id ?? this.bundleService.newBundleId();
    this.bundleMetaForm = this.fb.group({
      name: ['', [Validators.maxLength(255)]]
    });
    this.bundleContentForm = this.fb.group({
      protocolTemplates: this.fb.array([]),
      protocolCommands: this.fb.array([])
    });
    if (data.bundle) {
      this.bundleMetaForm.patchValue({ name: data.bundle.name ?? '' });
    }
  }

  ngAfterViewInit(): void {
    /** 仅做一次：避免切换 Tab 时第二个编辑器挂载触发 changes 再次 patch，把用户已改内容刷回打开对话框时的快照 */
    let initialPatchDone = false;
    const patchFromDialogData = (): boolean => {
      if (initialPatchDone) {
        return true;
      }
      const editor = this.bundleEditorRefs?.first;
      if (!editor) {
        return false;
      }
      const b = this.data.bundle;
      if (b) {
        editor.patchProtocolTemplatesFromModel(deepClone(b.protocolTemplates ?? b.monitoringTemplates ?? []));
        editor.patchProtocolCommandsFromModel(deepClone(b.protocolCommands ?? b.monitoringCommands ?? []));
      } else {
        editor.patchProtocolTemplatesFromModel([]);
        editor.patchProtocolCommandsFromModel([]);
      }
      initialPatchDone = true;
      return true;
    };
    const tryPatch = (attempt: number) => {
      if (patchFromDialogData()) {
        return;
      }
      if (attempt < 20) {
        setTimeout(() => tryPatch(attempt + 1), 0);
      }
    };
    setTimeout(() => tryPatch(0), 0);
  }

  cancel(): void {
    this.dialogRef.close(undefined);
  }

  /** 载入华诺 J3000+ 示例帧模板与命令（0x21 下行参区为 paN_hi/paN_lo；兼容 paN_word；含 0xA2 第二匹配） */
  applyHuanuoJ3000Preset(): void {
    const name = String(this.bundleMetaForm.get('name')?.value ?? '').trim() || 'J3000+';
    const preset = buildHuanuoJ3000PresetBundle(name);
    this.bundleMetaForm.patchValue({ name: preset.name ?? name }, { emitEvent: false });
    const patch = (): boolean => {
      const editor = this.bundleEditorRefs?.first;
      if (!editor) {
        return false;
      }
      editor.patchProtocolTemplatesFromModel(deepClone(preset.protocolTemplates));
      editor.patchProtocolCommandsFromModel(deepClone(preset.protocolCommands));
      return true;
    };
    if (!patch()) {
      setTimeout(() => {
        if (!patch()) {
          setTimeout(() => patch(), 0);
        }
      }, 0);
    }
  }

  save(): void {
    const displayName = String(this.bundleMetaForm.get('name')?.value ?? '').trim();
    const templates = this.serializeTemplates();
    const commands = this.serializeCommands();
    const tLen = templates.length;
    const cLen = commands.length;
    if (tLen > 0 && cLen === 0 || tLen === 0 && cLen > 0) {
      this.store.dispatch(new ActionNotificationShow({
        message: this.translate.instant('profiles.protocol-templates-save-validation'),
        type: 'warn',
        duration: 4000,
        verticalPosition: 'top',
        horizontalPosition: 'right'
      }));
      return;
    }
    if ((tLen > 0 || cLen > 0) && !displayName) {
      this.store.dispatch(new ActionNotificationShow({
        message: this.translate.instant('profiles.protocol-templates-name-required'),
        type: 'warn',
        duration: 4000,
        verticalPosition: 'top',
        horizontalPosition: 'right'
      }));
      return;
    }
    const bundle: ProtocolTemplateBundle = {
      id: this.bundleId,
      name: displayName || undefined,
      protocolTemplates: templates,
      protocolCommands: commands
    };
    this.dialogRef.close(bundle);
  }

  private serializeTemplates(): ProtocolTemplateDefinition[] {
    const displayName = String(this.bundleMetaForm.get('name')?.value ?? '').trim();
    const tplArr = this.bundleContentForm.get('protocolTemplates') as UntypedFormArray;
    const tplRows = (tplArr?.getRawValue() ?? []) as Array<Record<string, unknown>>;
    const row = tplRows[0];
    if (!row) {
      return [];
    }
    /** 与命令行 templateId 一致：优先表单隐藏字段 id，避免仅改展示名时与命令引用错位 */
    const templateId = String(row['id'] ?? '').trim() || displayName;
    if (!templateId) {
      return [];
    }
    const fieldRows = (row['hexProtocolFields'] as Array<Record<string, unknown>>) ?? [];
    const fields = this.fieldsFromHexRows(fieldRows);
    const t: ProtocolTemplateDefinition = {
      id: templateId,
      commandByteOffset: Number(row['commandByteOffset']) ?? 12,
      commandMatchWidth: Number(row['commandMatchWidth']) === 1 ? 1 : 4
    };
    const csType = String(row['checksumType'] ?? 'NONE').trim();
    if (csType && csType.toUpperCase() !== 'NONE') {
      const cs: TcpHexChecksumDefinition = {
        type: csType,
        fromByte: Number(row['checksumFromByte']) || 0,
        toExclusive: Number(row['checksumToExclusive']) || 0,
        checksumByteIndex: Number(row['checksumByteIndex']) ?? -2
      };
      t.checksum = cs;
    }
    if (fields.length) {
      t.hexProtocolFields = fields;
    }
    return [t];
  }

  private serializeCommands(): ProtocolTemplateCommandDefinition[] {
    const commands: ProtocolTemplateCommandDefinition[] = [];
    const cmdArr = this.bundleContentForm.get('protocolCommands') as UntypedFormArray;
    const cmdRows = (cmdArr?.getRawValue() ?? []) as Array<Record<string, unknown>>;
    for (const row of cmdRows) {
      const tid = String(row['templateId'] ?? '').trim();
      if (!tid) {
        continue;
      }
      const cmd: ProtocolTemplateCommandDefinition = {
        templateId: tid,
        commandValue: this.parseCommandValue(row['commandValue']),
        matchValueType: row['matchValueType'] as TcpHexValueType,
        direction: row['direction'] as ProtocolTemplateCommandDirection
      };
      if (cmd.direction !== ProtocolTemplateCommandDirection.DOWNLINK) {
        const secOff = this.optionalFormNumber(row['secondaryMatchByteOffset']);
        if (secOff !== undefined && secOff >= 0) {
          cmd.secondaryMatchByteOffset = secOff;
          cmd.secondaryMatchValueType = (row['secondaryMatchValueType'] as TcpHexValueType) ?? TcpHexValueType.UINT8;
          cmd.secondaryMatchValue = this.parseCommandValue(row['secondaryMatchValue']);
        }
      }
      const cn = String(row['name'] ?? '').trim();
      if (cn) {
        cmd.name = cn;
      }
      if (this.isFormBooleanTrue(row['downlinkPayloadLengthAuto'])) {
        cmd.downlinkPayloadLengthAuto = true;
      }
      const lenFk = String(row['downlinkPayloadLengthFieldKey'] ?? '').trim();
      if (lenFk) {
        cmd.downlinkPayloadLengthFieldKey = lenFk;
      }
      const overrideRows = (row['overrideFields'] as Array<Record<string, unknown>>) ?? [];
      const overrides = this.fieldsFromHexRows(overrideRows);
      if (overrides.length) {
        cmd.fields = overrides;
      }
      commands.push(cmd);
    }
    return commands;
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
        if (this.isFormBooleanTrue(r['includeInDownlinkPayloadLength'])) {
          def.includeInDownlinkPayloadLength = true;
        }
        const mks = this.parseDownlinkPayloadLengthMemberKeys(r);
        if (mks?.length) {
          def.downlinkPayloadLengthMemberKeys = mks;
        }
        if (this.isFormBooleanTrue(r['autoDownlinkPayloadLength'])) {
          def.autoDownlinkPayloadLength = true;
        }
        // 与 TcpHexFieldDefinition.validate 一致：整型固定线值与 BYTES_AS_HEX 固定 hex 不能同时存在
        if (vt === TcpHexValueType.BYTES_AS_HEX) {
          const fixHex = String(r['fixedBytesHex'] ?? '').replace(/\s+/g, '');
          if (fixHex) {
            def.fixedBytesHex = fixHex;
          }
        } else {
          const fixIntTxt = String(r['fixedWireIntegralValueText'] ?? '').trim();
          if (fixIntTxt) {
            def.fixedWireIntegralValue = this.parseIntegralWireText(fixIntTxt);
          }
        }
        const dpe = this.optionalFormNumber(r['downlinkPayloadEndExclusiveByteOffset']);
        if (dpe !== undefined) {
          def.downlinkPayloadEndExclusiveByteOffset = dpe;
        }
        return def;
      });
  }

  /**
   * 支持十进制、0x 前缀十六进制；纯十六进制数字串（含 A–F）按十六进制解析；纯数字串按十进制。
   */
  /** 十进制、0x 十六进制；与 parseCommandValue 一致，结果为可放入 Java long 的整数 */
  private parseIntegralWireText(raw: string): number {
    const n = parseIntegralWireTextToNumber(raw);
    return n !== undefined && Number.isFinite(n) ? Math.trunc(n) : 0;
  }

  private parseCommandValue(raw: unknown): number {
    if (raw === null || raw === undefined) {
      return 0;
    }
    if (typeof raw === 'number' && Number.isFinite(raw)) {
      return Math.trunc(raw);
    }
    const s = String(raw).trim();
    if (!s) {
      return 0;
    }
    if (/^0x[0-9a-fA-F]+$/i.test(s)) {
      const n = parseInt(s.slice(2), 16);
      return Number.isFinite(n) ? n : 0;
    }
    if (/^[0-9a-fA-F]+$/.test(s) && /[a-f]/i.test(s)) {
      const n = parseInt(s, 16);
      return Number.isFinite(n) ? n : 0;
    }
    const n = Number(s);
    return Number.isFinite(n) ? Math.trunc(n) : 0;
  }

  /** mat-checkbox / 部分控件在 getRawValue 中可能为 true 或非严格类型 */
  private isFormBooleanTrue(value: unknown): boolean {
    return value === true || value === 'true' || value === 1 || value === '1';
  }

  private optionalFormNumber(value: unknown): number | undefined {
    if (value === null || value === undefined || value === '') {
      return undefined;
    }
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }

  private parseDownlinkPayloadLengthMemberKeys(r: Record<string, unknown>): string[] | undefined {
    const direct = r['downlinkPayloadLengthMemberKeys'];
    if (Array.isArray(direct)) {
      const out = direct.map(x => String(x).trim()).filter(Boolean);
      return out.length ? out.slice(0, 64) : undefined;
    }
    const text = String(r['downlinkPayloadLengthMemberKeysText'] ?? '').trim();
    if (!text) {
      return undefined;
    }
    const parts = text.split(/[\s,;]+/).map(s => s.trim()).filter(Boolean);
    return parts.length ? parts.slice(0, 64) : undefined;
  }
}
