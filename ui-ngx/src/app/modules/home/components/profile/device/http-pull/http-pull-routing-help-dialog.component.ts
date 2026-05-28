///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'tb-http-pull-routing-help-dialog',
  templateUrl: './http-pull-routing-help-dialog.component.html',
  styleUrls: ['./http-pull-routing-help-dialog.component.scss']
})
export class HttpPullRoutingHelpDialogComponent {

  readonly sampleJson = `{
  "code": 0,
  "data": {
    "list": [
      { "deviceId": "sensor-001", "temperature": 25.3 },
      { "deviceId": "sensor-002", "temperature": 22.1 }
    ]
  }
}`;

  constructor(public dialogRef: MatDialogRef<HttpPullRoutingHelpDialogComponent>) {}
}
