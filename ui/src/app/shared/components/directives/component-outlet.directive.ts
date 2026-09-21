import {
  ComponentRef,
  Directive, EventEmitter, Injector,
  Input,
  OnChanges, Output, Renderer2,
  SimpleChange,
  SimpleChanges,
  Type,
  ViewContainerRef
} from '@angular/core';

@Directive({
  // eslint-disable-next-line @angular-eslint/directive-selector
  selector: '[jnksIotComponentOutlet]',
  exportAs: 'jnksIotComponentOutlet'
})
export class JnksIotComponentOutletDirective<_T = unknown> implements OnChanges {
  private componentRef: ComponentRef<any> | null = null;
  private context = new JnksIotComponentOutletContext();
  @Input() jnksIotComponentOutletContext: any | null = null;
  @Input() jnksIotComponentStyle: { [klass: string]: any } | null = null;
  @Input() jnksIotComponentInjector: Injector | null = null;
  @Input() jnksIotComponentOutlet: Type<any> = null;
  @Output() componentChange = new EventEmitter<ComponentRef<any>>();

  static ngTemplateContextGuard<T>(
    _dir: JnksIotComponentOutletDirective<T>,
    _ctx: any
  ): _ctx is JnksIotComponentOutletContext {
    return true;
  }

  private recreateComponent(): void {
    this.viewContainer.clear();
    this.componentRef = this.viewContainer.createComponent(this.jnksIotComponentOutlet, {index: 0, injector: this.jnksIotComponentInjector});
    this.componentChange.next(this.componentRef);
    if (this.jnksIotComponentOutletContext) {
      for (const propName of Object.keys(this.jnksIotComponentOutletContext)) {
        this.componentRef.instance[propName] = this.jnksIotComponentOutletContext[propName];
      }
    }
    if (this.jnksIotComponentStyle) {
      for (const propName of Object.keys(this.jnksIotComponentStyle)) {
        this.renderer.setStyle(this.componentRef.location.nativeElement, propName, this.jnksIotComponentStyle[propName]);
      }
    }
  }

  private updateContext(): void {
    const newCtx = this.jnksIotComponentOutletContext;
    const oldCtx = this.componentRef.instance as any;
    if (newCtx) {
      for (const propName of Object.keys(newCtx)) {
        oldCtx[propName] = newCtx[propName];
      }
    }
  }

  constructor(private viewContainer: ViewContainerRef,
              private renderer: Renderer2) {}

  ngOnChanges(changes: SimpleChanges): void {
    const { jnksIotComponentOutletContext, jnksIotComponentOutlet } = changes;
    const shouldRecreateComponent = (): boolean => {
      let shouldOutletRecreate = false;
      if (jnksIotComponentOutlet) {
        if (jnksIotComponentOutlet.firstChange) {
          shouldOutletRecreate = true;
        } else {
          const isPreviousOutletTemplate = jnksIotComponentOutlet.previousValue instanceof Type;
          const isCurrentOutletTemplate = jnksIotComponentOutlet.currentValue instanceof Type;
          shouldOutletRecreate = isPreviousOutletTemplate || isCurrentOutletTemplate;
        }
      }
      const hasContextShapeChanged = (ctxChange: SimpleChange): boolean => {
        const prevCtxKeys = Object.keys(ctxChange.previousValue || {});
        const currCtxKeys = Object.keys(ctxChange.currentValue || {});
        if (prevCtxKeys.length === currCtxKeys.length) {
          for (const propName of currCtxKeys) {
            if (prevCtxKeys.indexOf(propName) === -1) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
      };
      const shouldContextRecreate =
        jnksIotComponentOutletContext && hasContextShapeChanged(jnksIotComponentOutletContext);
      return shouldContextRecreate || shouldOutletRecreate;
    };

    if (jnksIotComponentOutlet) {
      this.context.$implicit = jnksIotComponentOutlet.currentValue;
    }

    const recreateComponent = shouldRecreateComponent();
    if (recreateComponent) {
      this.recreateComponent();
    } else {
      this.updateContext();
    }
  }
}

export class JnksIotComponentOutletContext {
  public $implicit: any;
}
