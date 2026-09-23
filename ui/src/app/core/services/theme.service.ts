import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { LocalStorageService } from '@core/local-storage/local-storage.service';

/**
 * 两套主题：
 *   'saas' — SaaS 分析风（浅色，默认）
 *   'dark' — 暗黑
 *
 * 实现方式：两套主题的令牌都写成了 CSS 自定义属性（见 scss/constants.scss），
 * 两套 Material 主题也都编译出来了（见 theme.scss）。所以“切换”只是给 <body>
 * 加/去一个类，不需要重新编译或重载。
 *
 * <body> 上**始终**带 `jnks-iot-default`（Tailwind 的 important 作用域、
 * styles.scss/form.scss 的联合作用域、以及 5 个组件 scss 都依赖它），
 * 暗色时**额外**加 `jnks-iot-dark`。
 */
export type JnksIotTheme = 'saas' | 'dark';

export const JNKS_IOT_THEME_STORAGE_KEY = 'uiTheme';
export const JNKS_IOT_DARK_CLASS = 'jnks-iot-dark';

@Injectable({ providedIn: 'root' })
export class ThemeService {

  private readonly themeSubject: BehaviorSubject<JnksIotTheme>;
  readonly theme$: Observable<JnksIotTheme>;

  constructor(private localStorageService: LocalStorageService) {
    this.themeSubject = new BehaviorSubject<JnksIotTheme>(ThemeService.readStoredTheme());
    this.theme$ = this.themeSubject.asObservable();
    this.apply(this.themeSubject.value);
  }

  /** 读持久化的偏好；读不到或值不合法就回落到默认的 saas。 */
  static readStoredTheme(): JnksIotTheme {
    try {
      return localStorage.getItem('TB-' + JNKS_IOT_THEME_STORAGE_KEY) &&
             JSON.parse(localStorage.getItem('TB-' + JNKS_IOT_THEME_STORAGE_KEY)) === 'dark'
        ? 'dark' : 'saas';
    } catch {
      return 'saas';
    }
  }

  get theme(): JnksIotTheme {
    return this.themeSubject.value;
  }

  get isDark(): boolean {
    return this.theme === 'dark';
  }

  toggle(): void {
    this.set(this.isDark ? 'saas' : 'dark');
  }

  set(theme: JnksIotTheme): void {
    if (theme === this.themeSubject.value) {
      return;
    }
    this.localStorageService.setItem(JNKS_IOT_THEME_STORAGE_KEY, theme);
    this.themeSubject.next(theme);
    this.apply(theme);
  }

  private apply(theme: JnksIotTheme): void {
    const dark = theme === 'dark';
    document.body.classList.toggle(JNKS_IOT_DARK_CLASS, dark);
    this.syncDashboardPages(dark);
  }

  /**
   * 把主题同步到 `.jnks-iot-dashboard-page` 上。
   *
   * 时间序列图表（widget/lib/chart/time-series-chart.ts）判断暗色靠的是
   * **那个元素上的 `dark` 类**，而且自带一个 MutationObserver 监听它 —— 所以
   * 只把类挂在 <body> 上，已经在页面上的图表不会跟着变。这里补上这一步，
   * 复用图表本来就有的监听，不改图表代码。
   *
   * （新渲染出来的图表则由 time-series-chart.ts 里对 body 的回退判断兜住。）
   */
  private syncDashboardPages(dark: boolean): void {
    document.querySelectorAll('.jnks-iot-dashboard-page').forEach((el) => {
      el.classList.toggle('dark', dark);
    });
  }
}
