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
  ProtocolTemplateCommandDirection
} from '@shared/models/device.models';
import {
  buildDownlinkFieldValuesSkeleton,
  mergeTemplateAndCommandFields
} from '@home/pages/profiles/protocol-template-downlink-fields.util';

export interface ProtocolTemplateHexTestDialogData {
  bundles: ProtocolTemplateBundle[];
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
  hexTestBuildValuesJson = '{}';
  hexTestBuilding = false;
  hexTestBuildResult: ProtocolTemplateHexBuildResult | null = null;

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

  hexTestBundleLabel(b: ProtocolTemplateBundle): string {
    const name = b.name?.trim();
    if (name) {
      return `${name} (${b.id})`;
    }
    const tid = b.protocolTemplates?.[0]?.id ?? b.monitoringTemplates?.[0]?.id;
    return tid ? `${tid} (${b.id})` : b.id;
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
    const skeleton = buildDownlinkFieldValuesSkeleton(merged);
    this.hexTestBuildValuesJson = Object.keys(skeleton).length
      ? JSON.stringify(skeleton, null, 2)
      : '{}';
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

  runHexTestBuild(): void {
    const id = this.hexTestBundleId?.trim();
    const cmd = this.hexTestBuildCommand;
    if (!id || !cmd) {
      return;
    }
    let values: Record<string, unknown> = {};
    try {
      const parsed = JSON.parse(this.hexTestBuildValuesJson || '{}');
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        values = parsed as Record<string, unknown>;
      } else {
        throw new Error('not an object');
      }
    } catch {
      this.store.dispatch(new ActionNotificationShow({
        message: this.translate.instant('profiles.protocol-hex-test-values-json-invalid'),
        type: 'error',
        duration: 4000,
        verticalPosition: 'top',
        horizontalPosition: 'right'
      }));
      return;
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
