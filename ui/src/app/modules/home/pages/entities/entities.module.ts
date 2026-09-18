import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '@shared/shared.module';
import { EntitiesRoutingModule } from '@home/pages/entities/entities-routing.module';

@NgModule({
  declarations: [],
  imports: [
    CommonModule,
    SharedModule,
    EntitiesRoutingModule
  ]
})
export class EntitiesModule { }
