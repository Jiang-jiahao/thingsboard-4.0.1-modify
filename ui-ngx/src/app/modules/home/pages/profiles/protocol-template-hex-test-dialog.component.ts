///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import {
  ProtocolTemplateBundleService,
  ProtocolTemplateHexBuildResult,
  ProtocolTemplateHexParseResult
} from '@core/services/protocol-template-bundle.service';
import { ActionNotificationShow } from '@core/notification/notification.actions';
import { TranslateService } from '@ngx-translate/core';
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  TcpHexValueType
} from '@shared/models/device.models';
import {
  buildDownlinkFieldValuesSkeleton,
  defaultDownlinkFieldInputText,
  defaultJsonValueForHexField,
  formatDownlinkFieldEcho,
  listDownlinkEditableHexFields,
  mergeTemplateAndCommandFields,
  parseDownlinkFieldInput
} from '@home/pages/profiles/protocol-template-downlink-fields.util';

export interface ProtocolTemplateHexTestDialogData {
  bundles: ProtocolTemplateBundle[];
}

export interface HexTestBuildFieldRow {
  key: string;
  valueType: TcpHexValueType;
  byteLength?: number;
  inputText: string;
}

@Component({
  selector: 'tb-protocol-template-hex-test-dialog',
  templateUrl: './protocol-template-hex-test-dialog.component.html',
  styleUrls: ['./protocol-template-hex-test-dialog.component.scss']
})
export class ProtocolTemplateHexTestDialogComponent {

  hexTestBundleId: string | null = null;
  hexTestHexInput = '';
  hexTestParsing = false;
  hexTestParseResult: ProtocolTemplateHexParseResult | null = null;
  hexTestBuildCommand: ProtocolTemplateCommandDefinition | null = null;
  /** 无可编辑字段时的完整 JSON（十进制等，与旧版一致） */
  hexTestBuildValuesJson = '{}';
  /** 有字段表单时与各行合并 */
  hexTestBuildExtraJson = '{}';
  hexTestBuildFieldRows: HexTestBuildFieldRow[] = [];
  hexTestBuilding = false;
  hexTestBuildResult: ProtocolTemplateHexBuildResult | null = null;

  readonly TcpHexValueType = TcpHexValueType;

  readonly sameDownlinkCmd = (a: ProtocolTemplateCommandDefinition | null, b: ProtocolTemplateCommandDefinition | null): boolean =>
    !!(a && b && a.templateId === b.templateId && a.commandValue === b.commandValue && a.direction === b.direction);

  readonly bundles: ProtocolTemplateBundle[];

  constructor(
    private dialogRef: MatDialogRef<ProtocolTemplateHexTestDialogComponent, void>,
    @Inject(MAT_DIALOG_DATA) data: ProtocolTemplateHexTestDialogData,
    private bundleService: ProtocolTemplateBundleService,
    private store: Store<AppState>,
    private translate: TranslateService
  ) {
    this.bundles = (data?.bundles ?? []).slice();
    if (this.bundles.length) {
      this.hexTestBundleId = this.bundles[0].id;
      this.onHexTestBundleChange();
    }
  }

  close(): void {
    this.dialogRef.close();
  }

  get hexTestUsesFieldInputs(): boolean {
    return this.hexTestBuildFieldRows.length > 0;
  }

  hexTestBundleLabel(b: ProtocolTemplateBundle): string {
    const name = b.name?.trim();
    if (name) {
      return `${name} (${b.id})`;
    }
    const tid = b.protocolTemplates?.[0]?.id ?? b.monitoringTemplates?.[0]?.id;
    return tid ? `${tid} (${b.id})` : b.id;
  }

  /** 与协议模板编辑器命令值回显一致：0x 小写十六进制 */
  formatCommandHex(v: number | null | undefined): string {
    if (v === undefined || v === null || !Number.isFinite(Number(v))) {
      return '0x0';
    }
    const n = Math.trunc(Number(v));
    const u = n >= 0 ? n : n >>> 0;
    return '0x' + u.toString(16);
  }

  trackByBuildFieldKey(_index: number, row: HexTestBuildFieldRow): string {
    return row.key;
  }

  onHexTestBundleChange(): void {
    this.hexTestParseResult = null;
    this.hexTestBuildResult = null;
    const cmds = this.downlinkCommandsForHexTest();
    if (!this.hexTestBuildCommand || !cmds.some(c => this.sameDownlinkCmd(c, this.hexTestBuildCommand))) {
      this.hexTestBuildCommand = cmds.length ? cmds[0] : null;
    }
    this.applyBuildValuesSkeleton();
  }

  onHexTestBuildCommandChange(): void {
    this.hexTestBuildResult = null;
    this.applyBuildValuesSkeleton();
  }

  private applyBuildValuesSkeleton(): void {
    const bundle = this.bundles.find(x => x.id === this.hexTestBundleId);
    const cmd = this.hexTestBuildCommand;
    this.hexTestBuildFieldRows = [];
    this.hexTestBuildExtraJson = '{}';
    if (!bundle || !cmd) {
      this.hexTestBuildValuesJson = '{}';
      return;
    }
    const templates = bundle.protocolTemplates ?? bundle.monitoringTemplates ?? [];
    const tpl = templates.find(t => t && t.id === cmd.templateId);
    if (!tpl) {
      this.hexTestBuildValuesJson = '{}';
      return;
    }
    const merged = mergeTemplateAndCommandFields(tpl.hexProtocolFields, cmd.fields);
    const cmdOff = tpl.commandByteOffset ?? 12;
    const opts = {
      commandByteOffset: cmdOff,
      commandMatchValueType: cmd.matchValueType ?? TcpHexValueType.UINT32_LE,
      command: cmd
    };
    const fields = listDownlinkEditableHexFields(merged, opts);
    const skeleton = buildDownlinkFieldValuesSkeleton(merged, opts);
    for (const f of fields) {
      const dj = defaultJsonValueForHexField(f);
      if (dj === undefined) {
        continue;
      }
      this.hexTestBuildFieldRows.push({
        key: f.key!,
        valueType: f.valueType,
        byteLength: f.byteLength,
        inputText: defaultDownlinkFieldInputText(f, dj)
      });
    }
    if (this.hexTestBuildFieldRows.length === 0) {
      this.hexTestBuildValuesJson = Object.keys(skeleton).length
        ? JSON.stringify(skeleton, null, 2)
        : '{}';
    } else {
      this.hexTestBuildValuesJson = '{}';
    }
  }

  downlinkCommandsForHexTest(): ProtocolTemplateCommandDefinition[] {
    const b = this.bundles.find(x => x.id === this.hexTestBundleId);
    if (!b) {
      return [];
    }
    const cmds = b.protocolCommands ?? b.monitoringCommands ?? [];
    return cmds.filter(c => c && (c.direction === ProtocolTemplateCommandDirection.DOWNLINK
      || c.direction === ProtocolTemplateCommandDirection.BOTH));
  }

  runHexTestParse(): void {
    const id = this.hexTestBundleId?.trim();
    if (!id || !this.hexTestHexInput.trim()) {
      return;
    }
    this.hexTestParsing = true;
    this.hexTestParseResult = null;
    this.bundleService.parseHex(id, this.hexTestHexInput).subscribe({
      next: (r) => {
        this.hexTestParseResult = r;
        this.hexTestParsing = false;
      },
      error: () => {
        this.hexTestParsing = false;
        this.hexTestParseResult = {
          success: false,
          errorMessage: this.translate.instant('profiles.protocol-hex-parse-http-error')
        };
      }
    });
  }

  hexTestTelemetryJson(): string {
    const t = this.hexTestParseResult?.telemetry;
    if (!t) {
      return '';
    }
    try {
      return JSON.stringify(t, null, 2);
    } catch {
      return String(t);
    }
  }

  private showBuildError(message: string): void {
    this.store.dispatch(new ActionNotificationShow({
      message,
      type: 'error',
      duration: 4500,
      verticalPosition: 'top',
      horizontalPosition: 'right'
    }));
  }

  runHexTestBuild(): void {
    const id = this.hexTestBundleId?.trim();
    const cmd = this.hexTestBuildCommand;
    if (!id || !cmd) {
      return;
    }
    let values: Record<string, unknown>;
    if (this.hexTestUsesFieldInputs) {
      const built: Record<string, unknown> = {};
      for (const row of this.hexTestBuildFieldRows) {
        const parsed = parseDownlinkFieldInput(row.inputText, row.valueType, row.byteLength);
        if (parsed.ok !== true) {
          this.showBuildError(this.translate.instant('profiles.protocol-hex-test-field-parse-error', {
            key: row.key,
            detail: parsed.error
          }));
          return;
        }
        built[row.key] = parsed.value;
        row.inputText = formatDownlinkFieldEcho(parsed.value, row.valueType, parsed.echoMode, row.byteLength);
      }
      let extra: Record<string, unknown> = {};
      try {
        const parsed = JSON.parse(this.hexTestBuildExtraJson || '{}');
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          extra = parsed as Record<string, unknown>;
        } else {
          throw new Error('not an object');
        }
      } catch {
        this.showBuildError(this.translate.instant('profiles.protocol-hex-test-extra-json-invalid'));
        return;
      }
      values = { ...built, ...extra };
    } else {
      try {
        const parsed = JSON.parse(this.hexTestBuildValuesJson || '{}');
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          values = parsed as Record<string, unknown>;
        } else {
          throw new Error('not an object');
        }
      } catch {
        this.showBuildError(this.translate.instant('profiles.protocol-hex-test-values-json-invalid'));
        return;
      }
    }
    this.hexTestBuilding = true;
    this.hexTestBuildResult = null;
    this.bundleService.buildHex({
      bundleId: id,
      commandValue: cmd.commandValue,
      templateId: cmd.templateId,
      values
    }).subscribe({
      next: (r) => {
        this.hexTestBuildResult = r;
        this.hexTestBuilding = false;
      },
      error: () => {
        this.hexTestBuilding = false;
        this.hexTestBuildResult = {
          success: false,
          errorMessage: this.translate.instant('profiles.protocol-hex-test-build-http-error')
        };
      }
    });
  }
}
