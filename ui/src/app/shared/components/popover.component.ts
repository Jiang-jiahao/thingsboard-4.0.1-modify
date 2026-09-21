import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ComponentRef,
  Directive,
  ElementRef,
  EventEmitter,
  Injector,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  Optional,
  Output,
  Renderer2,
  SimpleChanges,
  TemplateRef,
  Type,
  ViewChild,
  ViewContainerRef,
  ViewEncapsulation
} from '@angular/core';
import { Direction, Directionality } from '@angular/cdk/bidi';
import {
  CdkConnectedOverlay,
  CdkOverlayOrigin,
  ConnectedOverlayPositionChange,
  ConnectionPositionPair,
  NoopScrollStrategy
} from '@angular/cdk/overlay';
import { Subject, Subscription } from 'rxjs';
import {
  convertStrictPopoverPlacement,
  DEFAULT_POPOVER_POSITIONS,
  getPlacementName,
  isStrictPopoverPlacement,
  popoverMotion,
  PopoverPlacement,
  PopoverPreferredPlacement,
  PropertyMapping,
  StrictPopoverPlacement
} from '@shared/components/popover.models';
import { POSITION_MAP } from '@shared/models/overlay.models';
import { distinctUntilChanged, take, takeUntil } from 'rxjs/operators';
import { isNotEmptyStr, onParentScrollOrWindowResize } from '@core/utils';
import { animate, AnimationBuilder, AnimationMetadata, style } from '@angular/animations';
import { coerceBoolean } from '@shared/decorators/coercion';

export type JnksIotPopoverTrigger = 'click' | 'focus' | 'hover' | null;

@Directive({
  // eslint-disable-next-line @angular-eslint/directive-selector
  selector: '[jnks-iot-popover]',
  exportAs: 'jnksIotPopover',
  // eslint-disable-next-line @angular-eslint/no-host-metadata-property
  host: {
    '[class.jnks-iot-popover-open]': 'visible'
  }
})
export class JnksIotPopoverDirective implements OnChanges, OnDestroy, AfterViewInit {

  /* eslint-disable @angular-eslint/no-input-rename */
  @Input('jnksIotPopoverContent') content?: string | TemplateRef<void>;
  @Input('jnksIotPopoverContext') context?: any | null = null;
  @Input('jnksIotPopoverTrigger') trigger?: JnksIotPopoverTrigger = 'hover';
  @Input('jnksIotPopoverPlacement') placement?: string | string[] = 'top';
  @Input('jnksIotPopoverOrigin') origin?: ElementRef<HTMLElement>;
  @Input('jnksIotPopoverVisible') visible?: boolean;
  @Input('jnksIotPopoverShowCloseButton') @coerceBoolean() showCloseButton = true;
  @Input('jnksIotPopoverMouseEnterDelay') mouseEnterDelay?: number;
  @Input('jnksIotPopoverMouseLeaveDelay') mouseLeaveDelay?: number;
  @Input('jnksIotPopoverOverlayClassName') overlayClassName?: string;
  @Input('jnksIotPopoverOverlayStyle') overlayStyle?: { [klass: string]: any };
  @Input() jnksIotPopoverBackdrop = false;

  // eslint-disable-next-line @angular-eslint/no-output-rename
  @Output('jnksIotPopoverVisibleChange') readonly visibleChange = new EventEmitter<boolean>();

  component?: JnksIotPopoverComponent;

  private readonly destroy$ = new Subject<void>();
  private readonly triggerDisposables: Array<() => void> = [];
  private delayTimer?;
  private internalVisible = false;

  constructor(
    private elementRef: ElementRef,
    private hostView: ViewContainerRef,
    private renderer: Renderer2
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    const { trigger } = changes;

    if (trigger && !trigger.isFirstChange()) {
      this.registerTriggers();
    }

    if (this.component) {
      this.updatePropertiesByChanges(changes);
    }
  }

  ngAfterViewInit(): void {
    this.registerTriggers();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.clearTogglingTimer();
    this.removeTriggerListeners();
  }

  show(): void {
    if (!this.component) {
      this.createComponent();
    }
    this.component?.show();
  }

  hide(): void {
    this.component?.hide();
  }

  updatePosition(): void {
    if (this.component) {
      this.component.updatePosition();
    }
  }

  private createComponent(): void {
    const componentRef = this.hostView.createComponent(JnksIotPopoverComponent);

    this.component = componentRef.instance;

    this.renderer.removeChild(
      this.renderer.parentNode(this.elementRef.nativeElement),
      componentRef.location.nativeElement
    );
    this.component.setOverlayOrigin(new CdkOverlayOrigin(this.origin || this.elementRef));

    this.initProperties();

    this.component.jnksIotVisibleChange
      .pipe(distinctUntilChanged(), takeUntil(this.destroy$))
      .subscribe((visible: boolean) => {
        this.internalVisible = visible;
        this.visibleChange.emit(visible);
      });
  }

  private registerTriggers(): void {
    // When the method gets invoked, all properties has been synced to the dynamic component.
    // After removing the old API, we can just check the directive's own `nzTrigger`.
    const el = this.elementRef.nativeElement;
    const trigger = this.trigger;

    this.removeTriggerListeners();

    if (trigger === 'hover') {
      let overlayElement: HTMLElement;
      this.triggerDisposables.push(
        this.renderer.listen(el, 'mouseenter', () => {
          this.delayEnterLeave(true, true, this.mouseEnterDelay);
        })
      );
      this.triggerDisposables.push(
        this.renderer.listen(el, 'mouseleave', () => {
          this.delayEnterLeave(true, false, this.mouseLeaveDelay);
          if (this.component?.overlay.overlayRef && !overlayElement) {
            overlayElement = this.component.overlay.overlayRef.overlayElement;
            this.triggerDisposables.push(
              this.renderer.listen(overlayElement, 'mouseenter', () => {
                this.delayEnterLeave(false, true, this.mouseEnterDelay);
              })
            );
            this.triggerDisposables.push(
              this.renderer.listen(overlayElement, 'mouseleave', () => {
                this.delayEnterLeave(false, false, this.mouseLeaveDelay);
              })
            );
          }
        })
      );
    } else if (trigger === 'focus') {
      this.triggerDisposables.push(this.renderer.listen(el, 'focusin', () => this.show()));
      this.triggerDisposables.push(this.renderer.listen(el, 'focusout', () => this.hide()));
    } else if (trigger === 'click') {
      this.triggerDisposables.push(
        this.renderer.listen(el, 'click', (e: MouseEvent) => {
          e.preventDefault();
          if (this.component?.visible) {
            this.hide();
          } else {
            this.show();
          }
        })
      );
    }
    // Else do nothing because user wants to control the visibility programmatically.
  }

  private updatePropertiesByChanges(changes: SimpleChanges): void {
    this.updatePropertiesByKeys(Object.keys(changes));
  }

  private updatePropertiesByKeys(keys?: string[]): void {
    const mappingProperties: PropertyMapping = {
      // common mappings
      content: ['jnksIotContent', () => this.content],
      context: ['jnksIotComponentContext', () => this.context],
      trigger: ['jnksIotTrigger', () => this.trigger],
      placement: ['jnksIotPlacement', () => this.placement],
      visible: ['jnksIotVisible', () => this.visible],
      showCloseButton: ['jnksIotShowCloseButton', () => this.showCloseButton],
      mouseEnterDelay: ['jnksIotMouseEnterDelay', () => this.mouseEnterDelay],
      mouseLeaveDelay: ['jnksIotMouseLeaveDelay', () => this.mouseLeaveDelay],
      overlayClassName: ['jnksIotOverlayClassName', () => this.overlayClassName],
      overlayStyle: ['jnksIotOverlayStyle', () => this.overlayStyle],
      jnksIotPopoverBackdrop: ['jnksIotBackdrop', () => this.jnksIotPopoverBackdrop]
    };

    (keys || Object.keys(mappingProperties).filter(key => !key.startsWith('directive'))).forEach(
      (property: any) => {
        if (mappingProperties[property]) {
          const [name, valueFn] = mappingProperties[property];
          this.updateComponentValue(name, valueFn());
        }
      }
    );

    this.component?.updateByDirective();
  }


  private initProperties(): void {
    this.updatePropertiesByKeys();
  }

  private updateComponentValue(key: string, value: any): void {
    if (typeof value !== 'undefined') {
      // @ts-ignore
      this.component[key] = value;
    }
  }

  private delayEnterLeave(isOrigin: boolean, isEnter: boolean, delay: number = -1): void {
    if (this.delayTimer) {
      this.clearTogglingTimer();
    } else if (delay > 0) {
      this.delayTimer = setTimeout(() => {
        this.delayTimer = undefined;
        if (isEnter) {
          this.show();
        } else {
          this.hide();
        }
      }, delay * 1000);
    } else {
      // `isOrigin` is used due to the tooltip will not hide immediately
      // (may caused by the fade-out animation).
      if (isEnter && isOrigin) {
        this.show();
      } else {
        this.hide();
      }
    }
  }

  private removeTriggerListeners(): void {
    this.triggerDisposables.forEach(dispose => dispose());
    this.triggerDisposables.length = 0;
  }

  private clearTogglingTimer(): void {
    if (this.delayTimer) {
      clearTimeout(this.delayTimer);
      this.delayTimer = undefined;
    }
  }
}

@Component({
  selector: 'jnks-iot-popover',
  exportAs: 'jnksIotPopoverComponent',
  animations: [popoverMotion],
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None,
  styleUrls: ['./popover.component.scss'],
  template: `
    <ng-template
      #overlay="cdkConnectedOverlay"
      cdkConnectedOverlay
      [cdkConnectedOverlayHasBackdrop]="hasBackdrop"
      [cdkConnectedOverlayBackdropClass]="backdropClass"
      [cdkConnectedOverlayOrigin]="origin"
      [cdkConnectedOverlayPositions]="positions"
      [cdkConnectedOverlayScrollStrategy]="scrollStrategy"
      [cdkConnectedOverlayOpen]="visible"
      [cdkConnectedOverlayPush]="!strictPosition"
      [cdkConnectedOverlayFlexibleDimensions]="strictPosition"
      (overlayOutsideClick)="onClickOutside($event)"
      (detach)="hide()"
      (positionChange)="onPositionChange($event)"
    >
      <div #popoverRoot [@popoverMotion]="jnksIotAnimationState"
           (@popoverMotion.done)="animationDone()">
        <div
          #popover
          class="jnks-iot-popover"
          [class.strict-position]="strictPosition"
          [class.jnks-iot-popover-rtl]="dir === 'rtl'"
          [class]="classMap"
          [style]="jnksIotOverlayStyle"
        >
          <div class="jnks-iot-popover-content">
            <div class="jnks-iot-popover-arrow">
              <span class="jnks-iot-popover-arrow-content"></span>
            </div>
            <div class="jnks-iot-popover-inner" [style]="jnksIotPopoverInnerStyle" role="tooltip">
              <div *ngIf="jnksIotShowCloseButton" class="jnks-iot-popover-close-button" (click)="closeButtonClick($event)">×</div>
              <div style="width: 100%; height: 100%;">
                <div class="jnks-iot-popover-inner-content"  [style]="jnksIotPopoverInnerContentStyle"
                     [class.strict-position]="strictPosition">
                  <ng-container *ngIf="jnksIotContent">
                    <ng-container *jnksIotStringTemplateOutlet="jnksIotContent; context: jnksIotComponentContext">
                      {{ jnksIotContent }}
                    </ng-container>
                  </ng-container>
                  <ng-container *ngIf="jnksIotComponent"
                                [jnksIotComponentOutlet]="jnksIotComponent"
                                [jnksIotComponentInjector]="jnksIotComponentInjector"
                                [jnksIotComponentOutletContext]="jnksIotComponentContext"
                                (componentChange)="onComponentChange($event)"
                                [jnksIotComponentStyle]="jnksIotComponentStyle">
                  </ng-container>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </ng-template>
  `
})
export class JnksIotPopoverComponent<T = any> implements OnDestroy, OnInit {

  @ViewChild('overlay', { static: false }) overlay!: CdkConnectedOverlay;
  @ViewChild('popoverRoot', { static: false }) popoverRoot!: ElementRef<HTMLElement>;
  @ViewChild('popover', { static: false }) popover!: ElementRef<HTMLElement>;

  jnksIotContent: string | TemplateRef<void> | null = null;
  jnksIotComponent: Type<T> | null = null;
  jnksIotComponentRef: ComponentRef<T> | null = null;
  jnksIotComponentContext: any;
  jnksIotComponentInjector: Injector | null = null;
  jnksIotComponentStyle: { [klass: string]: any }  = {};
  jnksIotOverlayClassName!: string;
  jnksIotPopoverInnerStyle: { [klass: string]: any } = {};
  jnksIotPopoverInnerContentStyle: { [klass: string]: any } = {};
  jnksIotBackdrop = false;
  jnksIotMouseEnterDelay?: number;
  jnksIotMouseLeaveDelay?: number;
  jnksIotHideOnClickOutside = true;
  jnksIotShowCloseButton = true;
  jnksIotModal = false;

  jnksIotAnimationState = 'active';

  jnksIotHideStart = new Subject<void>();
  jnksIotVisibleChange = new Subject<boolean>();
  jnksIotAnimationDone = new Subject<void>();
  jnksIotComponentChange = new Subject<ComponentRef<any>>();
  jnksIotDestroy = new Subject<void>();

  set jnksIotVisible(value: boolean) {
    const visible = value;
    if (this.visible !== visible) {
      this.visible = visible;
      this.jnksIotVisibleChange.next(visible);
    }
  }

  get jnksIotVisible(): boolean {
    return this.visible && this.jnksIotAnimationState === 'active';
  }

  visible = false;

  set jnksIotHidden(value: boolean) {
    const hidden = value;
    if (this.hidden !== hidden) {
      this.hidden = hidden;
      if (this.hidden) {
        this.renderer.setStyle(this.popoverRoot.nativeElement, 'width', this.popoverRoot.nativeElement.offsetWidth + 'px');
        this.renderer.setStyle(this.popoverRoot.nativeElement, 'height', this.popoverRoot.nativeElement.offsetHeight + 'px');
      } else {
        setTimeout(() => {
          this.renderer.removeStyle(this.popoverRoot.nativeElement, 'width');
          this.renderer.removeStyle(this.popoverRoot.nativeElement, 'height');
        });
      }
      this.updateStyles();
      this.cdr.markForCheck();
    }
  }

  get jnksIotHidden(): boolean {
    return this.hidden;
  }

  hidden = false;
  lastIsIntersecting = true;

  set jnksIotTrigger(value: JnksIotPopoverTrigger) {
    this.trigger = value;
  }

  get jnksIotTrigger(): JnksIotPopoverTrigger {
    return this.trigger;
  }

  protected trigger: JnksIotPopoverTrigger = 'hover';

  set jnksIotPlacement(value: PopoverPreferredPlacement) {
    if (typeof value === 'string') {
      if (isStrictPopoverPlacement(value)) {
        const placement = convertStrictPopoverPlacement(value as StrictPopoverPlacement);
        this.positions = [POSITION_MAP[placement]];
        this.strictPosition = true;
      } else {
        this.positions = [POSITION_MAP[value], ...DEFAULT_POPOVER_POSITIONS];
      }
    } else {
      if (value.length && isStrictPopoverPlacement(value[0])) {
        this.positions = value.map((val: any) => POSITION_MAP[convertStrictPopoverPlacement(val)]);
        this.strictPosition = true;
      } else {
        const preferredPosition = value.map(placement => POSITION_MAP[placement]);
        this.positions = [...preferredPosition, ...DEFAULT_POPOVER_POSITIONS];
      }
    }
  }

  get hasBackdrop(): boolean {
    return this.jnksIotModal || (this.jnksIotTrigger === 'click' && this.jnksIotBackdrop);
  }

  get backdropClass(): string {
    return this.jnksIotModal ? 'jnks-iot-popover-overlay-backdrop' : '';
  }


  set jnksIotOverlayStyle(value: { [klass: string]: any }) {
    this._tbOverlayStyle = value;
    if (this.popover) {
      this.cdr.detectChanges();
    }
  }

  get jnksIotOverlayStyle(): { [klass: string]: any } {
    return this._tbOverlayStyle;
  }

  preferredPlacement: PopoverPlacement = 'top';
  strictPosition = false;
  origin!: CdkOverlayOrigin;
  public dir: Direction = 'ltr';
  classMap: { [klass: string]: any } = {};
  positions: ConnectionPositionPair[] = [...DEFAULT_POPOVER_POSITIONS];
  scrollStrategy = new NoopScrollStrategy();
  private parentScrollSubscription: Subscription = null;
  private intersectionObserver = new IntersectionObserver((entries) => {
    if (this.lastIsIntersecting !== entries[0].isIntersecting) {
      this.lastIsIntersecting = entries[0].isIntersecting;
      this.updateStyles();
      this.cdr.markForCheck();
    }
  }, {threshold: [0.5]});
  private _tbOverlayStyle: { [klass: string]: any } = {};

  constructor(
    public cdr: ChangeDetectorRef,
    private renderer: Renderer2,
    private animationBuilder: AnimationBuilder,
    @Optional() private directionality: Directionality
  ) {}

  ngOnInit(): void {
    this.directionality.change?.pipe(takeUntil(this.jnksIotDestroy)).subscribe((direction: Direction) => {
      this.dir = direction;
      this.cdr.detectChanges();
    });

    this.dir = this.directionality.value;
  }

  ngOnDestroy(): void {
    if (this.parentScrollSubscription) {
      this.parentScrollSubscription.unsubscribe();
      this.parentScrollSubscription = null;
    }
    if (this.origin) {
      const el = this.origin.elementRef.nativeElement;
      this.intersectionObserver.unobserve(el);
    }
    this.intersectionObserver.disconnect();
    this.intersectionObserver = null;
    this.jnksIotHideStart.complete();
    this.jnksIotVisibleChange.complete();
    this.jnksIotAnimationDone.complete();
    this.jnksIotDestroy.next();
    this.jnksIotDestroy.complete();
  }

  closeButtonClick($event: Event) {
    if ($event) {
      $event.preventDefault();
      $event.stopPropagation();
    }
    this.hide();
  }

  show(): void {
    if (this.jnksIotVisible) {
      return;
    }

    if (!this.isEmpty()) {
      this.jnksIotVisible = true;
      this.jnksIotVisibleChange.next(true);
      this.cdr.detectChanges();
    }

    if (this.origin && this.overlay && this.overlay.overlayRef) {
      if (this.overlay.overlayRef.getDirection() === 'rtl') {
        this.overlay.overlayRef.setDirection('ltr');
      }
      const el = this.origin.elementRef.nativeElement;
      this.parentScrollSubscription = onParentScrollOrWindowResize(el).subscribe(() => {
        this.overlay.overlayRef.updatePosition();
      });
      this.intersectionObserver.observe(el);
    }
    this.jnksIotAnimationState = 'active';
  }

  hide(): void {
    if (!this.jnksIotVisible) {
      return;
    }
    this.jnksIotHideStart.next();
    if (this.parentScrollSubscription) {
      this.parentScrollSubscription.unsubscribe();
      this.parentScrollSubscription = null;
    }
    if (this.origin) {
      const el = this.origin.elementRef.nativeElement;
      this.intersectionObserver.unobserve(el);
    }
    this.jnksIotAnimationState = 'void';
    this.cdr.detectChanges();
    this.jnksIotAnimationDone.pipe(take(1)).subscribe(() => {
      this.jnksIotVisible = false;
      this.cdr.detectChanges();
    });
  }

  updateByDirective(): void {
    this.updateStyles();
    this.cdr.detectChanges();

    Promise.resolve().then(() => {
      this.updatePosition();
      this.updateVisibilityByContent();
    });
  }

  resize(width: string, height: string, animationDurationMs?: number) {
    if (animationDurationMs && animationDurationMs > 0) {
      const prevWidth = this.popover.nativeElement.offsetWidth;
      const prevHeight = this.popover.nativeElement.offsetHeight;
      const animationMetadata: AnimationMetadata[] = [style({width: prevWidth + 'px', height: prevHeight + 'px'}),
        animate(animationDurationMs + 'ms', style({width, height}))];
      const factory = this.animationBuilder.build(animationMetadata);
      const player = factory.create(this.popover.nativeElement);
      player.play();
      const resize$ = new ResizeObserver(() => {
        this.updatePosition();
      });
      resize$.observe(this.popover.nativeElement);
      player.onDone(() => {
        player.destroy();
        resize$.disconnect();
        this.setSize(width, height);
      });
    } else {
      this.setSize(width, height);
    }
  }

  private setSize(width: string, height: string) {
    this.renderer.setStyle(this.popover.nativeElement, 'width', width);
    this.renderer.setStyle(this.popover.nativeElement, 'height', height);
    this.updatePosition();
  }

  updatePosition(): void {
    if (this.origin && this.overlay && this.overlay.overlayRef) {
      this.overlay.overlayRef.updatePosition();
    }
  }

  onPositionChange(position: ConnectedOverlayPositionChange): void {
    this.preferredPlacement = getPlacementName(position);
    this.updateStyles();
    this.cdr.detectChanges();
  }

  updateStyles(): void {
    this.classMap = {
      [`jnks-iot-popover-placement-${this.preferredPlacement}`]: true,
      ['jnks-iot-popover-hidden']: this.jnksIotHidden || !this.lastIsIntersecting
    };
    if (this.jnksIotOverlayClassName) {
      this.classMap[this.jnksIotOverlayClassName] = true;
    }
  }

  setOverlayOrigin(origin: CdkOverlayOrigin): void {
    this.origin = origin;
    this.cdr.markForCheck();
  }

  onClickOutside(event: MouseEvent): void {
    if (!this.jnksIotModal && this.jnksIotHideOnClickOutside && !this.origin.elementRef.nativeElement.contains(event.target) && this.jnksIotTrigger !== null) {
      if (!this.isTopOverlay(event.target as Element)) {
        this.hide();
      }
    }
  }

  onComponentChange(component: ComponentRef<any>) {
    this.jnksIotComponentRef = component;
    if (this.strictPosition) {
      this.renderer.setStyle(this.jnksIotComponentRef.location.nativeElement, 'display', 'flex');
      this.renderer.setStyle(this.jnksIotComponentRef.location.nativeElement, 'height', '100%');
    }
    this.jnksIotComponentChange.next(component);
  }

  animationDone() {
    this.jnksIotAnimationDone.next();
  }

  private isTopOverlay(targetElement: Element): boolean {
    const target = $(targetElement);
    if (target.parents('.cdk-overlay-container').length) {
      let targetOverlayContainerChild: JQuery<Element>;
      if (target.hasClass('cdk-overlay-backdrop')) {
        targetOverlayContainerChild = target;
      } else {
        targetOverlayContainerChild = target.parents('.cdk-overlay-pane').parent();
      }
      const currentOverlayContainerChild = $(this.overlay.overlayRef.overlayElement).parent();
      return targetOverlayContainerChild.index() > currentOverlayContainerChild.index();
    }
    return false;
  }

  private updateVisibilityByContent(): void {
    if (this.isEmpty()) {
      this.hide();
    }
  }

  private isEmpty(): boolean {
    return (this.jnksIotComponent instanceof Type || this.jnksIotContent instanceof TemplateRef)
      ? false : !isNotEmptyStr(this.jnksIotContent);
  }
}
