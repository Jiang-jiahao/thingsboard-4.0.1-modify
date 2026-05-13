///
/// Copyright © 2016-2025 The Thingsboard Authors
///
///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import {
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateDefinition
} from '@shared/models/device.models';

export type ParseImportedProtocolTemplateBundlesErrorKey =
  | 'invalid-json'
  | 'no-valid-bundle'
  | 'incomplete-bundle';

export type ParseImportedProtocolTemplateBundlesResult =
  | { ok: true; bundles: ProtocolTemplateBundle[] }
  | { ok: false; errorKey: ParseImportedProtocolTemplateBundlesErrorKey };

/**
 * 解析「导出 JSON」或手写 JSON：根可为单个对象或对象数组；兼容 monitoringTemplates / monitoringCommands。
 */
export function parseProtocolTemplateBundlesFromImportedJson(text: string): ParseImportedProtocolTemplateBundlesResult {
  let root: unknown;
  try {
    root = JSON.parse(text);
  } catch {
    return { ok: false, errorKey: 'invalid-json' };
  }
  const items: unknown[] = Array.isArray(root) ? root : [root];
  const bundles: ProtocolTemplateBundle[] = [];
  for (const item of items) {
    if (!item || typeof item !== 'object') {
      continue;
    }
    const o = item as Record<string, unknown>;
    const tpl = o['protocolTemplates'] ?? o['monitoringTemplates'];
    const cmd = o['protocolCommands'] ?? o['monitoringCommands'];
    if (!Array.isArray(tpl) || !Array.isArray(cmd)) {
      continue;
    }
    if (tpl.length === 0 && cmd.length === 0) {
      continue;
    }
    if (tpl.length === 0 || cmd.length === 0) {
      return { ok: false, errorKey: 'incomplete-bundle' };
    }
    bundles.push({
      id: '',
      name: typeof o['name'] === 'string' ? o['name'] : undefined,
      description: typeof o['description'] === 'string' ? o['description'] : undefined,
      protocolTemplates: tpl as ProtocolTemplateDefinition[],
      protocolCommands: cmd as ProtocolTemplateCommandDefinition[]
    });
  }
  if (bundles.length === 0) {
    return { ok: false, errorKey: 'no-valid-bundle' };
  }
  return { ok: true, bundles };
}
