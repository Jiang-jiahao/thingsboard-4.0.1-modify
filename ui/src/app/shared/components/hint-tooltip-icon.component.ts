import { Component, HostBinding, Input } from '@angular/core';
import { TooltipPosition } from '@angular/material/tooltip';

@Component({
  selector: '[jnks-iot-hint-tooltip-icon]',
  templateUrl: './hint-tooltip-icon.component.html',
  styleUrls: ['./hint-tooltip-icon.component.scss']
})
export class HintTooltipIconComponent {

  @HostBinding('class.jnks-iot-hint-tooltip')
  @Input('jnks-iot-hint-tooltip-icon') tooltipText: string;

  @Input()
  tooltipPosition: TooltipPosition = 'right';

  @Input()
  hintIcon = 'info';

}
