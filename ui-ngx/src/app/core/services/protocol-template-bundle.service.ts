///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import {
  LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY,
  PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY,
  ProtocolTemplateBundle
} from '@shared/models/device.models';
import { Tenant } from '@shared/models/tenant.model';
import { defaultHttpOptionsFromConfig, RequestConfig } from '@core/http/http-utils';

@Injectable({
  providedIn: 'root'
})
export class ProtocolTemplateBundleService {

  constructor(private http: HttpClient) {}

  /** 从数据库 REST 加载（首次访问时后端可将旧版 Tenant.additionalInfo 迁移入库） */
  getBundles(config?: RequestConfig): Observable<ProtocolTemplateBundle[]> {
    return this.http.get<ProtocolTemplateBundle[]>(
      '/api/protocolTemplateBundles',
      defaultHttpOptionsFromConfig(config));
  }

  saveBundle(bundle: ProtocolTemplateBundle, config?: RequestConfig): Observable<ProtocolTemplateBundle> {
    return this.http.post<ProtocolTemplateBundle>(
      '/api/protocolTemplateBundle',
      bundle,
      defaultHttpOptionsFromConfig(config));
  }

  deleteBundle(id: string, config?: RequestConfig): Observable<void> {
    return this.http.delete<void>(
      `/api/protocolTemplateBundle/${encodeURIComponent(id)}`,
      defaultHttpOptionsFromConfig(config));
  }

  /** 兼容：仍可从租户 additionalInfo 解析（例如离线脚本）；正常运行请使用 {@link getBundles} */
  parseBundlesFromTenant(tenant: Tenant | null | undefined): ProtocolTemplateBundle[] {
    const ai = tenant?.additionalInfo as Record<string, unknown> | undefined;
    const raw = ai?.[PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY] ?? ai?.[LEGACY_PROTOCOL_TEMPLATE_BUNDLES_ADDITIONAL_INFO_KEY];
    if (!Array.isArray(raw)) {
      return [];
    }
    const out: ProtocolTemplateBundle[] = [];
    for (const item of raw) {
      if (!item || typeof item !== 'object' || typeof (item as ProtocolTemplateBundle).id !== 'string') {
        continue;
      }
      const b = item as ProtocolTemplateBundle;
      const tpl = b.protocolTemplates ?? b.monitoringTemplates;
      const cmd = b.protocolCommands ?? b.monitoringCommands;
      out.push({
        id: b.id,
        name: typeof b.name === 'string' ? b.name : undefined,
        protocolTemplates: Array.isArray(tpl) ? tpl : [],
        protocolCommands: Array.isArray(cmd) ? cmd : []
      });
    }
    return out;
  }

  newBundleId(): string {
    return crypto.randomUUID();
  }

  parseHex(bundleId: string, hex: string, config?: RequestConfig): Observable<ProtocolTemplateHexParseResult> {
    return this.http.post<ProtocolTemplateHexParseResult>(
      '/api/protocolTemplateBundle/parseHex',
      { bundleId, hex },
      defaultHttpOptionsFromConfig(config));
  }

  buildHex(
    body: { bundleId: string; commandValue: number; templateId?: string; values?: Record<string, unknown> },
    config?: RequestConfig
  ): Observable<ProtocolTemplateHexBuildResult> {
    return this.http.post<ProtocolTemplateHexBuildResult>(
      '/api/protocolTemplateBundle/buildHex',
      body,
      defaultHttpOptionsFromConfig(config));
  }
}

/** 与后端 ProtocolTemplateHexParseResult 一致 */
export interface ProtocolTemplateHexParseResult {
  success: boolean;
  errorMessage?: string;
  telemetry?: Record<string, unknown>;
  matchedHexCommandProfile?: string;
}

/** 与后端 ProtocolTemplateHexBuildResult 一致 */
export interface ProtocolTemplateHexBuildResult {
  success: boolean;
  hex?: string;
  errorMessage?: string;
}
