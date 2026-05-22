///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { TranslateService } from '@ngx-translate/core';
import { mergeDeep } from '@core/utils';
import { Observable, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';

/**
 * Re-applies full gateway.* translations after gateway-management-extension loads.
 * The extension bundle calls its own locale merge and can leave English strings in zh_CN.
 */
@Injectable({ providedIn: 'root' })
export class GatewayLocaleMergeService {

  private readonly cache: Record<string, Record<string, unknown>> = {};

  constructor(private http: HttpClient,
              private translate: TranslateService) {}

  apply(lang?: string): Observable<void> {
    const targetLang = lang || this.translate.currentLang;
    if (!targetLang) {
      return of(void 0);
    }
    return this.loadGatewayLocale(targetLang).pipe(
      tap(gatewayLocale => {
        if (!gatewayLocale || !Object.keys(gatewayLocale).length) {
          return;
        }
        const overrides = this.cache[`${targetLang}:overrides`] || {};
        const merged = mergeDeep({}, gatewayLocale, overrides);
        this.translate.setTranslation(targetLang, { gateway: merged }, true);
      }),
      map(() => void 0)
    );
  }

  /**
   * Extension widgets init asynchronously; re-apply translations after they patch the catalog.
   */
  applyWithRetry(lang?: string): void {
    const targetLang = lang || this.translate.currentLang;
    this.loadAll(targetLang).subscribe(() => this.apply(targetLang).subscribe());
    for (const delay of [200, 800, 2000, 5000, 10000, 15000]) {
      setTimeout(() => {
        this.loadAll(targetLang).subscribe(() => this.apply(targetLang).subscribe());
      }, delay);
    }
  }

  private loadAll(lang: string): Observable<void> {
    return this.http.get<{ gateway: Record<string, unknown> }>(`/assets/locale/gateway-locale-overrides.${lang}.json`).pipe(
      tap(data => {
        if (data?.gateway) {
          this.cache[`${lang}:overrides`] = data.gateway;
        }
      }),
      catchError(() => {
        this.cache[`${lang}:overrides`] = {};
        return of(null);
      }),
      map(() => void 0)
    );
  }

  private loadGatewayLocale(lang: string): Observable<Record<string, unknown>> {
    if (this.cache[lang]) {
      return of(this.cache[lang]);
    }
    const url = `/assets/locale/gateway-locale.${lang}.json`;
    return this.http.get<{ gateway: Record<string, unknown> }>(url).pipe(
      map(data => data?.gateway || {}),
      tap(gateway => {
        if (gateway && Object.keys(gateway).length) {
          this.cache[lang] = gateway;
        }
      }),
      catchError(() => of(this.translate.translations[lang]?.gateway as Record<string, unknown> || {}))
    );
  }
}
