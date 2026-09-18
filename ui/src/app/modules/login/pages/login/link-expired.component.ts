import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { PageComponent } from '@shared/components/page.component';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'tb-link-expired',
  templateUrl: './link-expired.component.html',
  styleUrls: ['./link-expired.component.scss']
})
export class LinkExpiredComponent extends PageComponent {

  isPasswordLinkExpired: boolean;
  title: string;
  message: string;

  constructor(protected store: Store<AppState>,
              private route: ActivatedRoute,
              private router: Router) {
    super(store);
    this.isPasswordLinkExpired = this.route.snapshot.data.passwordLinkExpired;
    this.title = this.isPasswordLinkExpired ? 'login.reset-password-link-expired' : 'login.activation-link-expired';
    this.message = this.isPasswordLinkExpired ? 'login.reset-password-link-expired-message' :
      'login.activation-link-expired-message';
  }

  navigateToLoginPage() {
    this.router.navigateByUrl('login');
  }
}
