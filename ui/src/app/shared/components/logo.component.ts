import { Component } from '@angular/core';
import { map } from 'rxjs/operators';
import { environment as env } from '@env/environment';
import { ThemeService } from '@core/services/theme.service';

@Component({
  selector: 'jnks-iot-logo',
  templateUrl: './logo.component.html',
  styleUrls: ['./logo.component.scss']
})
export class LogoComponent {

  /**
   * 标识按主题取图：暗底用白字版，亮底用深字版。
   *
   * 仓库里原本**只有白字版**（`jnks-logo-white.png`），亮色主题下它会隐形 ——
   * 之前是用 CSS `filter: invert(1)` 或加一块深色底板顶着的。
   * 改成按主题换图之后，那两处 hack 都已删掉。
   *
   * 注意：`jnks-logo-dark.png` 是从白字版**反相生成的占位素材**，不是品牌原稿。
   */
  brandLogo$ = this.themeService.theme$.pipe(
    map((theme) => theme === 'dark' ? 'assets/jnks-logo-white.png' : 'assets/jnks-logo-dark.png')
  );

  brandName = env.brandName;
  brandSubtitle = env.appTitle;

  constructor(private themeService: ThemeService) {
  }

}
