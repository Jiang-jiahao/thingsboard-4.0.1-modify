import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { APP_INITIALIZER, NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { CoreModule } from '@core/core.module';
import { LoginModule } from '@modules/login/login.module';
import { HomeModule } from '@home/home.module';

import { AppComponent } from './app.component';
import { DashboardRoutingModule } from '@modules/dashboard/dashboard-routing.module';
import { RouterModule, Routes } from '@angular/router';

import { DefaultUrlSerializer, UrlSerializer, UrlTree } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { environment as env } from '@env/environment';
import { LocalStorageService } from '@core/local-storage/local-storage.service';
import { SETTINGS_KEY } from '@core/settings/settings.effects';
import { updateUserLang } from '@core/settings/settings.utils';
import { lastValueFrom, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

/**
 * 在路由初始导航之前把语言包加载好。
 *
 * 各实体的表格配置解析器（*-table-config.resolver）用 translate.instant() 把操作菜单等
 * 文案解析成字符串并固化（列标题存的是翻译键、由模板管道响应式翻译，所以不受影响）。
 * 解析器跑在语言包加载完成之前时，instant() 会取到默认语言（en_US）的文案并被永久固化，
 * 表现为首屏操作菜单中英混杂、硬刷新后仍是英文，而在应用内跳转一次就恢复正常。
 * 把加载提前到 bootstrap 之前可以根治这个竞态。
 */
export function initUserLanguage(translate: TranslateService, storageService: LocalStorageService): () => Promise<unknown> {
  return () => {
    translate.addLangs(env.supportedLangs);
    translate.setDefaultLang(env.defaultLang);
    const userLang = storageService.getItem(SETTINGS_KEY)?.userLang ?? null;
    return lastValueFrom(
      updateUserLang(translate, document, userLang).pipe(catchError(() => of(undefined)))
    );
  };
}

export default class TbUrlSerializer implements UrlSerializer {
  private _defaultUrlSerializer: DefaultUrlSerializer = new DefaultUrlSerializer();

  parse(url: string): UrlTree {
    // Encode parentheses
    url = url.replace(/\(/g, '%28').replace(/\)/g, '%29');
    // Use the default serializer.
    return this._defaultUrlSerializer.parse(url)
  }

  serialize(tree: UrlTree): string {
    return this._defaultUrlSerializer.serialize(tree).replace(/%28/g, '(').replace(/%29/g, ')');
  }
}

const routes: Routes = [
  { path: '**',
    redirectTo: 'home'
  }
];

@NgModule({
  imports: [
    RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class PageNotFoundRoutingModule { }


@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    AppRoutingModule,
    CoreModule,
    LoginModule,
    HomeModule,
    DashboardRoutingModule,
    PageNotFoundRoutingModule
  ],
  providers: [
    { provide: UrlSerializer, useClass: TbUrlSerializer },
    {
      provide: APP_INITIALIZER,
      useFactory: initUserLanguage,
      deps: [TranslateService, LocalStorageService],
      multi: true
    }
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
