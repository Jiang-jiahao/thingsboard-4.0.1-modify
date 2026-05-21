///
/// Copyright © 2016-2025 The Thingsboard Authors
///

import {
  buildDownlinkFieldValuesSkeleton,
  defaultDownlinkFieldInputText,
  defaultJsonValueForHexField,
  listDownlinkEditableHexFields,
  mergeTemplateAndCommandFields
} from '@home/pages/profiles/protocol-template-downlink-fields.util';
import {
  DeviceProfileRpcBindingType,
  DeviceProfileRpcMethod,
  isTcpHexVariableByteSlice,
  ProtocolTemplateBundle,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateDefinition,
  TcpHexFieldDefinition,
  TcpHexValueType,
  TcpDeviceProfileTransportConfiguration
} from '@shared/models/device.models';

export interface DeviceRpcInvokeFieldRow {
  /** 平台侧参数名（表单键） */
  platformKey: string;
  valueType: TcpHexValueType;
  byteLength?: number;
  inputText: string;
}

export function protocolTemplateBundleIdFromTcpProfile(
  tcp: TcpDeviceProfileTransportConfiguration | null | undefined
): string | null {
  const id = tcp?.transportTcpDataTypeConfiguration?.protocolTemplateBundleId
    ?? tcp?.transportTcpDataTypeConfiguration?.monitoringProtocolBundleId;
  return id?.trim() || null;
}

export function findBundleCommand(
  bundle: ProtocolTemplateBundle,
  method: DeviceProfileRpcMethod
): ProtocolTemplateCommandDefinition | null {
  const cmds = bundle.protocolCommands ?? bundle.monitoringCommands ?? [];
  return cmds.find(c => c
    && c.templateId === method.templateId
    && c.commandValue === method.commandValue
    && (method.templateCommandName == null || method.templateCommandName === '' || c.name === method.templateCommandName)
  ) ?? null;
}

export function platformKeyForTemplateField(templateKey: string, paramMap?: Record<string, string>): string {
  if (!paramMap) {
    return templateKey;
  }
  for (const [pk, tk] of Object.entries(paramMap)) {
    if (tk === templateKey) {
      return pk;
    }
  }
  return templateKey;
}

export function applyParamMapToTemplateValues(
  platformValues: Record<string, unknown>,
  paramMap?: Record<string, string>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(platformValues)) {
    const tk = paramMap?.[k] ?? k;
    out[tk] = v;
  }
  return out;
}

export function buildInvokeFieldRows(
  bundle: ProtocolTemplateBundle,
  catalogMethod: DeviceProfileRpcMethod
): { rows: DeviceRpcInvokeFieldRow[]; valuesJsonFallback: string; usesFieldInputs: boolean } {
  const cmd = findBundleCommand(bundle, catalogMethod);
  const templates = bundle.protocolTemplates ?? bundle.monitoringTemplates ?? [];
  const tpl = templates.find(t => t && t.id === catalogMethod.templateId);
  if (!cmd || !tpl) {
    return { rows: [], valuesJsonFallback: '{}', usesFieldInputs: false };
  }
  const merged = mergeTemplateAndCommandFields(tpl.hexProtocolFields, cmd.fields);
  const cmdOff = tpl.commandByteOffset ?? 12;
  const matchVt = cmd.matchValueType ?? TcpHexValueType.UINT32_LE;
  const tplCmdW = tpl.commandMatchWidth === 1 ? 1 : 4;
  const opts = {
    commandByteOffset: cmdOff,
    commandMatchValueType: matchVt,
    commandMatchWireByteWidth: isTcpHexVariableByteSlice(matchVt) ? tplCmdW : undefined,
    command: cmd
  };
  const fields = listDownlinkEditableHexFields(merged, opts);
  const skeleton = buildDownlinkFieldValuesSkeleton(merged, opts);
  const rows: DeviceRpcInvokeFieldRow[] = [];
  for (const f of fields) {
    const dj = defaultJsonValueForHexField(f);
    if (dj === undefined) {
      continue;
    }
    rows.push({
      platformKey: platformKeyForTemplateField(f.key!, catalogMethod.paramMap),
      valueType: f.valueType,
      byteLength: f.byteLength,
      inputText: defaultDownlinkFieldInputText(f, dj)
    });
  }
  if (rows.length > 0) {
    return { rows, valuesJsonFallback: '{}', usesFieldInputs: true };
  }
  const platformSkeleton: Record<string, unknown> = {};
  for (const [tk, v] of Object.entries(skeleton)) {
    platformSkeleton[platformKeyForTemplateField(tk, catalogMethod.paramMap)] = v;
  }
  return {
    rows: [],
    valuesJsonFallback: Object.keys(platformSkeleton).length ? JSON.stringify(platformSkeleton, null, 2) : '{}',
    usesFieldInputs: false
  };
}

export function parseNativeParamsJson(text: string): Record<string, unknown> | null {
  const t = (text ?? '').trim();
  if (!t) {
    return {};
  }
  try {
    const v = JSON.parse(t);
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      return v as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

export function mergeNativeParams(
  templateJson: string | undefined,
  userJson: Record<string, unknown>
): Record<string, unknown> {
  let base: Record<string, unknown> = {};
  const t = (templateJson ?? '').trim();
  if (t) {
    try {
      const v = JSON.parse(t);
      if (v && typeof v === 'object' && !Array.isArray(v)) {
        base = { ...(v as Record<string, unknown>) };
      }
    } catch {
      /* ignore invalid template */
    }
  }
  return { ...base, ...userJson };
}

export function catalogMethodLabel(m: DeviceProfileRpcMethod): string {
  return m.displayName?.trim() || m.id;
}

export function isTcpTemplateBinding(m: DeviceProfileRpcMethod): boolean {
  return m.bindingType === DeviceProfileRpcBindingType.TCP_TEMPLATE;
}
