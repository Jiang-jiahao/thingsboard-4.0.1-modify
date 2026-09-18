import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '@shared/shared.module';
import { HomeComponentsModule } from '@home/components/home-components.module';
import { ProfilesRoutingModule } from './profiles-routing.module';
import { ProtocolTemplateBundlesPageComponent } from './protocol-template-bundles-page.component';
import { ProtocolTemplateBundleDialogComponent } from './protocol-template-bundle-dialog.component';
import { ProtocolTemplateHexTestDialogComponent } from './protocol-template-hex-test-dialog.component';

@NgModule({
  declarations: [
    ProtocolTemplateBundlesPageComponent,
    ProtocolTemplateBundleDialogComponent,
    ProtocolTemplateHexTestDialogComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    HomeComponentsModule,
    ProfilesRoutingModule
  ]
})
export class ProfilesModule { }
