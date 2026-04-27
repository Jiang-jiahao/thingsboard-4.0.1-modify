///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { AfterViewInit, Component, Inject, OnDestroy, QueryList, ViewChildren } from '@angular/core';
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
  TcpHexValueType,
  isTcpHexVariableByteSlice,
  migrateLegacyLtvTagValueType
} from '@shared/models/device.models';
import { ProtocolTemplateBundleEditorComponent } from '@home/components/profile/device/protocol-template-bundle-editor.component';
import {
  normalizeFixedBytesHexWhitespace,
  parseIntegralWireTextToNumber,
  parseLtvTagWireTextToNumber,
  utf8FixedBytesFormValueToStoredFixedHex,
  unknownTagTelemetryKeyHexLiteralFromMappings
} from '@home/pages/profiles/protocol-template-downlink-fields.util';
import { Subscription } from 'rxjs';

export interface ProtocolTemplateBundleDialogData {
  bundle: ProtocolTemplateBundle | null;
  isNew: boolean;
}

@Component({
  selector: 'tb-protocol-template-bundle-dialog',
  templateUrl: './protocol-template-bundle-dialog.component.html',
  styleUrls: ['./protocol-template-bundle-dialog.component.scss']
})
export class ProtocolTemplateBundleDialogComponent implements AfterViewInit, OnDestroy {

  /** 对话框内「帧模板 / 命令」各有一个编辑器实例，共享同一 bundleContentForm；序列化必须读父表单，不能依赖某一个子组件引用。 */
  @ViewChildren(ProtocolTemplateBundleEditorComponent)
  private bundleEditorRefs!: QueryList<ProtocolTemplateBundleEditorComponent>;

  private bundleEditorsChangesSub?: Subscription;

  readonly isNew: boolean;
  bundleId: string;

  bundleMetaForm: UntypedFormGroup;
  bundleContentForm: UntypedFormGroup;

  /** 悬停「说明」字段时展示字数限制 */
  get descriptionDialogFullTooltip(): string {
    return this.translate.instant('profiles.protocol-templates-bundle-description-hint');
  }

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
      name: ['', [Validators.maxLength(255)]],
      description: ['', [Validators.maxLength(512)]]
    });
    this.bundleContentForm = this.fb.group({
      protocolTemplates: this.fb.array([]),
      protocolCommands: this.fb.array([])
    });
    if (data.bundle) {
      this.bundleMetaForm.patchValue({
        name: data.bundle.name ?? '',
        description: data.bundle.description ?? ''
      });
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
      this.bundleEditorsChangesSub?.unsubscribe();
      this.bundleEditorsChangesSub = undefined;
      return true;
    };
    if (patchFromDialogData()) {
      return;
    }
    /** mat-tab 懒加载时子组件可能晚于 ngAfterViewInit 才挂载，需订阅 QueryList.changes */
    this.bundleEditorsChangesSub = this.bundleEditorRefs.changes.subscribe(() => {
      patchFromDialogData();
    });
    let attempt = 0;
    const tryPatch = () => {
      if (patchFromDialogData()) {
        return;
      }
      if (attempt++ < 60) {
        setTimeout(tryPatch, 0);
      }
    };
    setTimeout(tryPatch, 0);
  }

  ngOnDestroy(): void {
    this.bundleEditorsChangesSub?.unsubscribe();
  }

  cancel(): void {
    this.dialogRef.close(undefined);
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
    if (this.bundleMetaForm.get('description')?.invalid) {
      this.store.dispatch(new ActionNotificationShow({
        message: this.translate.instant('profiles.protocol-templates-description-invalid'),
        type: 'warn',
        duration: 4000,
        verticalPosition: 'top',
        horizontalPosition: 'right'
      }));
      return;
    }
    const desc = String(this.bundleMetaForm.get('description')?.value ?? '').trim();
    const bundle: ProtocolTemplateBundle = {
      id: this.bundleId,
      name: displayName || undefined,
      protocolTemplates: templates,
      protocolCommands: commands
    };
    if (desc) {
      bundle.description = desc;
    }
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
    if (this.isFormBooleanTrue(row['hexLtvEnabled'])) {
      const ltvTagRows = (row['hexLtvTagMappings'] as Array<Record<string, unknown>>) ?? [];
      const tagMappings = this.ltvTagMappingsFromRows(ltvTagRows);
      const ltv: TcpHexLtvRepeatingConfig = {
        startByteOffset: Number(row['hexLtvStartOffset']) || 0,
        lengthFieldType: row['hexLtvLengthType'] as TcpHexValueType,
        tagFieldType: row['hexLtvTagType'] as TcpHexValueType,
        chunkOrder: (row['hexLtvChunkOrder'] as TcpHexLtvChunkOrder) ?? TcpHexLtvChunkOrder.LTV,
        keyPrefix: String(row['hexLtvKeyPrefix'] || 'ltv').trim() || 'ltv',
        unknownTagMode: (row['hexLtvUnknownMode'] as TcpHexUnknownTagMode) ?? TcpHexUnknownTagMode.SKIP
      };
      const maxItems = this.optionalFormNumber(row['hexLtvMaxItems']);
      if (maxItems !== undefined) {
        ltv.maxItems = maxItems;
      }
      if (this.isFormBooleanTrue(row['hexLtvLengthIncludesTag'])) {
        ltv.lengthIncludesTag = true;
      }
      if (this.isFormBooleanTrue(row['hexLtvLengthIncludesLengthField'])) {
        ltv.lengthIncludesLengthField = true;
      }
      if (tagMappings.length) {
        ltv.tagMappings = tagMappings;
      }
      ltv.unknownTagTelemetryKeyHexLiteral = unknownTagTelemetryKeyHexLiteralFromMappings(tagMappings);
      t.hexLtvRepeating = ltv;
    }
    return [t];
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

  private serializeCommands(): ProtocolTemplateCommandDefinition[] {
    const commands: ProtocolTemplateCommandDefinition[] = [];
    const cmdArr = this.bundleContentForm.get('protocolCommands') as UntypedFormArray;
    const cmdRows = (cmdArr?.getRawValue() ?? []) as Array<Record<string, unknown>>;
    for (const row of cmdRows) {
      const tid = String(row['templateId'] ?? '').trim();
      if (!tid) {
        continue;
      }
      const matchVt = row['matchValueType'] as TcpHexValueType;
      const cmd: ProtocolTemplateCommandDefinition = {
        templateId: tid,
        commandValue: isTcpHexVariableByteSlice(matchVt) ? 0 : this.parseCommandValue(row['commandValue']),
        matchValueType: matchVt,
        direction: row['direction'] as ProtocolTemplateCommandDirection
      };
      if (isTcpHexVariableByteSlice(matchVt)) {
        const hx = normalizeFixedBytesHexWhitespace(row['commandValue']);
        if (hx) {
          cmd.commandMatchBytesHex = hx;
        }
      }
      if (cmd.direction !== ProtocolTemplateCommandDirection.DOWNLINK) {
        const secOff = this.optionalFormNumber(row['secondaryMatchByteOffset']);
        if (secOff !== undefined && secOff >= 0) {
          cmd.secondaryMatchByteOffset = secOff;
          const secVt = (row['secondaryMatchValueType'] as TcpHexValueType) ?? TcpHexValueType.UINT8;
          cmd.secondaryMatchValueType = secVt;
          if (isTcpHexVariableByteSlice(secVt)) {
            cmd.secondaryMatchValue = 0;
            const shx = normalizeFixedBytesHexWhitespace(row['secondaryMatchValue']);
            if (shx) {
              cmd.secondaryMatchBytesHex = shx;
            }
          } else {
            cmd.secondaryMatchValue = this.parseCommandValue(row['secondaryMatchValue']);
          }
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
          if (mode === 'fixed') {
            let fixHex: string;
            if (vt === TcpHexValueType.BYTES_AS_UTF8) {
              fixHex = utf8FixedBytesFormValueToStoredFixedHex(String(r['fixedBytesHex'] ?? ''));
            } else {
              fixHex = normalizeFixedBytesHexWhitespace(r['fixedBytesHex']);
            }
            if (fixHex) {
              def.fixedBytesHex = fixHex;
            }
          }
        }
        if (this.isFormBooleanTrue(r['includeInDownlinkPayloadLength'])) {
          def.includeInDownlinkPayloadLength = true;
        }
        if (this.isFormBooleanTrue(r['autoDownlinkTotalFrameLength'])) {
          def.autoDownlinkTotalFrameLength = true;
        }
        if (this.isFormBooleanTrue(r['downlinkTotalFrameLengthExcludesLengthFieldBytes'])) {
          def.downlinkTotalFrameLengthExcludesLengthFieldBytes = true;
        }
        const mks = this.parseDownlinkPayloadLengthMemberKeys(r);
        if (mks?.length) {
          def.downlinkPayloadLengthMemberKeys = mks;
        }
        if (this.isFormBooleanTrue(r['autoDownlinkPayloadLength'])) {
          def.autoDownlinkPayloadLength = true;
        }
        if (!isTcpHexVariableByteSlice(vt)) {
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

  /** 固定线值整型：十进制解析（与 {@link parseIntegralWireTextToNumber} 一致）。 */
  private parseIntegralWireText(raw: string): number {
    const n = parseIntegralWireTextToNumber(raw);
    return n !== undefined && Number.isFinite(n) ? Math.trunc(n) : 0;
  }

  /** 命令主/次匹配期望值（整型类型）：仅十进制；字节切片匹配走 commandMatchBytesHex，不用本函数。 */
  private parseCommandValue(raw: unknown): number {
    if (raw === null || raw === undefined) {
      return 0;
    }
    if (typeof raw === 'number' && Number.isFinite(raw)) {
      return Math.trunc(raw);
    }
    const n = parseIntegralWireTextToNumber(raw);
    return n !== undefined ? n : 0;
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
