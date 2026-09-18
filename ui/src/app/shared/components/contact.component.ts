import { Component, Input } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';

@Component({
  selector: 'tb-contact',
  templateUrl: './contact.component.html'
})
export class ContactComponent {

  @Input()
  parentForm: UntypedFormGroup;

  @Input() isEdit: boolean;

  phoneInputDefaultCountry = 'US';

  constructor() {
  }

  changeCountry(countryCode: string) {
    this.phoneInputDefaultCountry = countryCode ?? 'US';
    setTimeout(() => {
      this.parentForm.get('phone').setValue(this.parentForm.get('phone').value);
    });
  }
}
