import { Component } from '@angular/core';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { DialogComponent } from '@app/shared/components/dialog.component';

export interface MoveWidgetsDialogResult {
  cols: number;
  rows: number;
}

@Component({
  selector: 'tb-move-widgets-dialog',
  templateUrl: './move-widgets-dialog.component.html',
  providers: [],
  styleUrls: []
})
export class MoveWidgetsDialogComponent extends DialogComponent<MoveWidgetsDialogComponent, MoveWidgetsDialogResult> {

  moveWidgetsFormGroup: UntypedFormGroup;

  constructor(protected store: Store<AppState>,
              protected router: Router,
              protected dialogRef: MatDialogRef<MoveWidgetsDialogComponent, MoveWidgetsDialogResult>,
              private fb: UntypedFormBuilder,
              private dialog: MatDialog) {
    super(store, router, dialogRef);

    this.moveWidgetsFormGroup = this.fb.group({
        cols: [0, [Validators.required]],
        rows: [0, [Validators.required]]
      }
    );
  }

  cancel(): void {
    this.dialogRef.close(null);
  }

  move(): void {
    const result: MoveWidgetsDialogResult = this.moveWidgetsFormGroup.value;
    this.dialogRef.close(result);
  }
}
