import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

const routes: Routes = [
  { path: '',
    redirectTo: 'home',
    pathMatch: 'full',
    data: {
      breadcrumb: {
        skip: true
      }
    }
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes,{
    useHash: false,
  })],
  exports: [RouterModule]
})
export class AppRoutingModule { }
