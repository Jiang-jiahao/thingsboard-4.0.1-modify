import { Component, Input } from '@angular/core';

@Component({
  selector: 'tb-example-hint',
  templateUrl: './example-hint.component.html',
  styleUrls: []
})
export class ExampleHintComponent {
  @Input() hintText: string;

  @Input() popupHelpLink: string;

  @Input() textAlign: string = 'left';
}


