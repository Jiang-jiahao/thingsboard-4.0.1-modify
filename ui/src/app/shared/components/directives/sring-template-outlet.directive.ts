import {
  Directive,
  EmbeddedViewRef,
  Input,
  OnChanges,
  SimpleChange,
  SimpleChanges,
  TemplateRef,
  ViewContainerRef
} from '@angular/core';

@Directive({
  // eslint-disable-next-line @angular-eslint/directive-selector
  selector: '[jnksIotStringTemplateOutlet]',
  exportAs: 'jnksIotStringTemplateOutlet'
})
export class JnksIotStringTemplateOutletDirective<_T = unknown> implements OnChanges {
  private embeddedViewRef: EmbeddedViewRef<any> | null = null;
  private context = new JnksIotStringTemplateOutletContext();
  @Input() jnksIotStringTemplateOutletContext: any | null = null;
  @Input() jnksIotStringTemplateOutlet: any | TemplateRef<any> = null;

  static ngTemplateContextGuard<T>(
    // eslint-disable-next-line @typescript-eslint/naming-convention,no-underscore-dangle,id-blacklist,id-match
    _dir: JnksIotStringTemplateOutletDirective<T>,
    // eslint-disable-next-line @typescript-eslint/naming-convention, no-underscore-dangle, id-blacklist, id-match
    _ctx: any
  ): _ctx is JnksIotStringTemplateOutletContext {
    return true;
  }

  private recreateView(): void {
    this.viewContainer.clear();
    const isTemplateRef = this.jnksIotStringTemplateOutlet instanceof TemplateRef;
    const templateRef = (isTemplateRef ? this.jnksIotStringTemplateOutlet : this.templateRef) as any;
    this.embeddedViewRef = this.viewContainer.createEmbeddedView(
      templateRef,
      isTemplateRef ? this.jnksIotStringTemplateOutletContext : this.context
    );
  }

  private updateContext(): void {
    const isTemplateRef = this.jnksIotStringTemplateOutlet instanceof TemplateRef;
    const newCtx = isTemplateRef ? this.jnksIotStringTemplateOutletContext : this.context;
    const oldCtx = this.embeddedViewRef.context as any;
    if (newCtx) {
      for (const propName of Object.keys(newCtx)) {
        oldCtx[propName] = newCtx[propName];
      }
    }
  }

  constructor(private viewContainer: ViewContainerRef, private templateRef: TemplateRef<any>) {}

  ngOnChanges(changes: SimpleChanges): void {
    const { jnksIotStringTemplateOutletContext, jnksIotStringTemplateOutlet } = changes;
    const shouldRecreateView = (): boolean => {
      let shouldOutletRecreate = false;
      if (jnksIotStringTemplateOutlet) {
        if (jnksIotStringTemplateOutlet.firstChange) {
          shouldOutletRecreate = true;
        } else {
          const isPreviousOutletTemplate = jnksIotStringTemplateOutlet.previousValue instanceof TemplateRef;
          const isCurrentOutletTemplate = jnksIotStringTemplateOutlet.currentValue instanceof TemplateRef;
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
        jnksIotStringTemplateOutletContext && hasContextShapeChanged(jnksIotStringTemplateOutletContext);
      return shouldContextRecreate || shouldOutletRecreate;
    };

    if (jnksIotStringTemplateOutlet) {
      this.context.$implicit = jnksIotStringTemplateOutlet.currentValue;
    }

    const recreateView = shouldRecreateView();
    if (recreateView) {
      this.recreateView();
    } else {
      this.updateContext();
    }
  }
}

export class JnksIotStringTemplateOutletContext {
  public $implicit: any;
}
