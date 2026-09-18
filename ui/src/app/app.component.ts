import 'hammerjs';

import { Component, OnInit } from '@angular/core';

import { environment as env } from '@env/environment';

import { TranslateService } from '@ngx-translate/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { LocalStorageService } from '@core/local-storage/local-storage.service';
import { DomSanitizer } from '@angular/platform-browser';
import { MatIconRegistry } from '@angular/material/icon';
import { getCurrentAuthState, selectUserReady } from '@core/auth/auth.selectors';
import { catchError, filter, map, shareReplay, skip, switchMap, tap } from 'rxjs/operators';
import { Observable, of } from 'rxjs';
import { AuthService } from '@core/auth/auth.service';
import { svgIcons, svgIconsUrl } from '@shared/models/icon.models';
import { ActionSettingsChangeLanguage } from '@core/settings/settings.actions';
import { updateUserLang } from '@core/settings/settings.utils';
import { SETTINGS_KEY } from '@core/settings/settings.effects';
import { initCustomJQueryEvents } from '@shared/models/jquery-event.models';

@Component({
  selector: 'tb-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {

  /** 当前语言包加载完成的信号，供首次导航前等待 */
  private userLangApplied$: Observable<unknown> = of(undefined);
  private loadingUserLang: string | null = null;

  constructor(private store: Store<AppState>,
              private storageService: LocalStorageService,
              private translate: TranslateService,
              private matIconRegistry: MatIconRegistry,
              private domSanitizer: DomSanitizer,
              private authService: AuthService) {

    console.log(`ThingsBoard Version: ${env.tbVersion}`);

    this.matIconRegistry.addSvgIconResolver((name, namespace) => {
      if (namespace === 'mdi') {
        return this.domSanitizer.bypassSecurityTrustResourceUrl(`./assets/mdi/${name}.svg`);
      } else {
        return null;
      }
    });

    for (const svgIcon of Object.keys(svgIcons)) {
      this.matIconRegistry.addSvgIconLiteral(
        svgIcon,
        this.domSanitizer.bypassSecurityTrustHtml(
          svgIcons[svgIcon]
        )
      );
    }

    for (const svgIcon of Object.keys(svgIconsUrl)) {
      this.matIconRegistry.addSvgIcon(svgIcon, this.domSanitizer.bypassSecurityTrustResourceUrl(svgIconsUrl[svgIcon]));
    }

    this.storageService.testLocalStorage();

    this.setupTranslate();
    this.setupAuth();

    initCustomJQueryEvents();
  }

  setupTranslate() {
    if (!env.production) {
      console.log(`Supported Langs: ${env.supportedLangs}`);
    }
    this.translate.addLangs(env.supportedLangs);
    if (!env.production) {
      console.log(`Default Lang: ${env.defaultLang}`);
    }
    this.translate.setDefaultLang(env.defaultLang);
  }

  setupAuth() {
    this.store.select(selectUserReady).pipe(
      filter((data) => data.isUserLoaded),
      tap((data) => {
        let userLang = getCurrentAuthState(this.store).userDetails?.additionalInfo?.lang ?? null;
        if (!userLang && !data.isAuthenticated) {
          const settings = this.storageService.getItem(SETTINGS_KEY);
          userLang = settings?.userLang ?? null;
        }
        this.loadUserLang(userLang);
        this.notifyUserLang(userLang);
      }),
      skip(1),
      // 首次导航前必须等语言包加载完成：路由解析器里的 translate.instant() 会把当时的
      // 语言固化成字符串写进表格列标题与操作菜单，早于加载完成就会一直显示默认语言。
      switchMap((data) => this.userLangApplied$.pipe(map(() => data))),
    ).subscribe((data) => {
      this.authService.gotoDefaultPlace(data.isAuthenticated);
    });
    this.authService.reloadUser();
  }

  /**
   * 主动持有语言包加载。translate.use() 在目标语言已是 currentLang 时会直接返回且不重新加载，
   * 若只靠 effect 触发，导航侧就拿不到「加载完成」的信号，所以这里自己发起并缓存同一份 observable。
   */
  private loadUserLang(userLang: string) {
    if (this.loadingUserLang === userLang) {
      return;
    }
    this.loadingUserLang = userLang;
    this.userLangApplied$ = updateUserLang(this.translate, document, userLang).pipe(
      // 语言包加载失败也必须放行导航，否则会卡在启动页
      catchError((err) => {
        console.error('Failed to load user language pack', err);
        return of(undefined);
      }),
      shareReplay(1)
    );
    this.userLangApplied$.subscribe();
  }

  ngOnInit() {
  }

  onActivateComponent($event: any) {
    const loadingElement = $('div#tb-loading-spinner');
    if (loadingElement.length) {
      loadingElement.remove();
    }
  }

  private notifyUserLang(userLang: string) {
    this.store.dispatch(new ActionSettingsChangeLanguage({userLang}));
  }

}
