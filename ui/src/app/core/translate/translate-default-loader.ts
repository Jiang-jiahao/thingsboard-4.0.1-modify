import { TranslateLoader } from '@ngx-translate/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class TranslateDefaultLoader implements TranslateLoader {

  constructor(private http: HttpClient) {

  }

  getTranslation(lang: string): Observable<object> {
    // 词条文件不是 hash 命名的，浏览器会一直吃旧缓存（表现为"新增词条显示成原始 key"）。
    // 带上构建时间戳，每次重建 URL 都变，强制重新拉取；nginx 侧 /assets/locale/ 另配了 no-cache 兜底。
    // @ts-ignore 由 esbuild define 在构建期注入
    const buildTs = typeof JNKS_IOT_BUILD_TS === 'undefined' ? '' : JNKS_IOT_BUILD_TS;
    return this.http.get(`assets/locale/locale.constant-${lang}.json?v=${buildTs}`);
  }
}
