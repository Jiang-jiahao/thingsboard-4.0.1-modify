#### Using existing JavaScript/Typescript code

<div class="divider"></div>
<br/>

Another approach of creating widgets is to use existing bundled JavaScript/Typescript code.

In this case, you can create own TypeScript class or Angular component and bundle it into the JnksIOT UI code.

In order to make this code accessible within the widget, you need to register corresponding Angular module or inject TypeScript class to a global variable (for ex. window object).

Some JnksIOT widgets already use this approach. Take a look at the `widget-component-service.ts`
or `widget-components.module.ts`. <br>
Here you can find how some bundled classes or components are registered for later use in JnksIOT widgets.

For example "Timeseries - Flot" widget (from "Charts" Widgets Bundle) uses `**JnksIotFlot**` TypeScript class which is injected as window property inside **widget-component-service.ts**:

```typescript
...

const widgetModulesTasks: Observable<any>[] = [];
...

widgetModulesTasks.push(from(import('@home/components/widget/lib/flot-widget')).pipe(
  tap((mod) => {
    (window as any).JnksIotFlot = mod.JnksIotFlot;
  }))
);
...

```

Another example is "Timeseries table" widget (from "Cards" Widgets Bundle) that uses Angular component `**jnks-iot-timeseries-table-widget**`<br>which is registered as dependency of **WidgetComponentsModule** Angular module inside **widget-components.module.ts**.
Thereby this component becomes available for use inside the widget template HTML.

```typescript
...

import { TimeseriesTableWidgetComponent } from '@home/components/widget/lib/timeseries-table-widget.component';

...

@NgModule({
  declarations:
    [
...
      TimeseriesTableWidgetComponent,
...
    ],
...
  exports: [
...
      TimeseriesTableWidgetComponent,
...
  ],
...    
})
export class WidgetComponentsModule { }
```

<br>
<br>
