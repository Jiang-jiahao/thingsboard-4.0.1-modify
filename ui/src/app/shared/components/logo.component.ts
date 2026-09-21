import { Component } from '@angular/core';
import { environment as env } from '@env/environment';

@Component({
  selector: 'tb-logo',
  templateUrl: './logo.component.html',
  styleUrls: ['./logo.component.scss']
})
export class LogoComponent {

  brandLogo = 'assets/jnks-logo-white.png';
  brandName = env.brandName;
  brandSubtitle = env.appTitle;

}
