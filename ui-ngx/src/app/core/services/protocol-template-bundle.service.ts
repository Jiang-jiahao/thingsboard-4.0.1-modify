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

  /**
   * 生成协议包 ID。优先使用 randomUUID；在 HTTP 页面或旧环境中 randomUUID 不可用，
   * 则用 getRandomValues 拼 UUID v4（与 randomUUID 语义一致）。
   */
  newBundleId(): string {
    const c = typeof globalThis !== 'undefined' ? globalThis.crypto : undefined;
    if (c?.randomUUID) {
      return c.randomUUID();
    }
    if (c?.getRandomValues) {
      const bytes = new Uint8Array(16);
      c.getRandomValues(bytes);
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const h = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
      return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, ch => {
      const r = (Math.random() * 16) | 0;
      const v = ch === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  parseHex(bundleId: string, hex: string, config?: RequestConfig): Observable<ProtocolTemplateHexParseResult> {
    return this.http.post<ProtocolTemplateHexParseResult>(
      '/api/protocolTemplateBundle/parseHex',
      { bundleId, hex },
      defaultHttpOptionsFromConfig(config));
  }

  buildHex(
    body: {
      bundleId: string;
      commandValue?: number | null;
      commandMatchBytesHex?: string | null;
      templateId?: string;
      values?: Record<string, unknown>;
    },
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
