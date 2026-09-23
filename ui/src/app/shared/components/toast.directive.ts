import {
  AnimationEvent,
  AnimationTriggerMetadata,
  animate,
  state,
  style,
  transition,
  trigger,
} from '@angular/animations';
import {
  AfterViewInit,
  ApplicationRef,
  Component,
  ComponentRef,
  Directive,
  ElementRef,
  EnvironmentInjector,
  HostBinding,
  HostListener,
  Inject,
  InjectionToken,
  Injector,
  NgZone,
  OnDestroy,
  createComponent,
} from '@angular/core';
import { NotificationMessage } from '@app/core/notification/notification.models';
import { Subscription } from 'rxjs';
import { ToastNotificationService } from '@core/services/toast-notification.service';
import { Clipboard } from '@angular/cdk/clipboard';
import Timeout = NodeJS.Timeout;

// 屏幕上同时最多挂几条，超了就把最早的一条顶掉
const MAX_STACKED_TOASTS = 4;

export const TOAST_DATA = new InjectionToken<ToastData>('JnksIotToastData');

interface ToastData {
  notification: NotificationMessage;
  // > 0 时显示底部倒计时条，并作为自动消失的时长；0/undefined 表示常驻
  progressMs?: number;
  onHover?: (paused: boolean) => void;
  destroyToastComponent: () => void;
}

interface StackedToast {
  msg: NotificationMessage;
  target: string;
  ref: ComponentRef<JnksIotSnackBarComponent>;
  slot: HTMLElement;
  timer: Timeout;
  // 关闭动画被打断时的兜底销毁
  killTimer: Timeout;
  deadline: number;
  remaining: number;
  total: number;
  paused: boolean;
  dismissed: boolean;
  removed: boolean;
}

// 全平台只有一个提示位置：屏幕正中。挂在 document.body 上，不受祖先元素的
// transform / overflow 影响（放组件里会跟着宿主跑，也就没法"都在中间"）。
// 页面上挂了好几处 [jnks-iot-toast] 宿主（home、dashboard-page、各部件与弹窗），
// 它们都只是订阅点，去重和渲染都归这一份管。
let toastStack: ToastStack = null;

class ToastStack {

  private container: HTMLElement = null;
  private items: StackedToast[] = [];

  constructor(private appRef: ApplicationRef,
              private envInjector: EnvironmentInjector,
              private ngZone: NgZone) {
  }

  show(notificationMessage: NotificationMessage): void {
    this.ngZone.run(() => {
      // 同一条已经挂在屏幕上了就不再摆一张（多个订阅点都会调进来）
      const visible = this.items.some(item => !item.dismissed
        && item.msg.message === notificationMessage.message
        && item.msg.type === notificationMessage.type);
      if (visible) {
        return;
      }
      const container = this.ensureContainer();
      const total = notificationMessage.duration > 0 ? notificationMessage.duration : 0;
      const slot = document.createElement('div');
      // 纵向排列 + 后加的排在最后：新的往下方排，整组始终居中
      container.appendChild(slot);

      const item: StackedToast = {
        msg: notificationMessage,
        target: notificationMessage.target || 'root',
        ref: null,
        slot,
        timer: null,
        killTimer: null,
        deadline: 0,
        remaining: 0,
        total,
        paused: false,
        dismissed: false,
        removed: false
      };
      const data: ToastData = {
        notification: notificationMessage,
        progressMs: total,
        onHover: (paused) => paused ? this.pause(item) : this.resume(item),
        destroyToastComponent: () => this.remove(item)
      };
      const injector = Injector.create({providers: [{provide: TOAST_DATA, useValue: data}]});
      item.ref = createComponent(JnksIotSnackBarComponent, {
        hostElement: slot,
        environmentInjector: this.envInjector,
        elementInjector: injector
      });
      this.appRef.attachView(item.ref.hostView);
      item.ref.changeDetectorRef.detectChanges();

      this.items.push(item);
      this.startTimer(item);

      // 只看还没进入关闭动画的那些，否则会一直数着正在退场的卡片
      while (this.items.filter(stacked => !stacked.dismissed).length > MAX_STACKED_TOASTS) {
        this.dismiss(this.items.find(stacked => !stacked.dismissed));
      }
    });
  }

  // 隐藏某个来源的提示（编辑器/部件关闭时会发），不给 target 就当成页面级的
  hide(target?: string): void {
    const wanted = target || 'root';
    this.ngZone.run(() => this.items
      .filter(item => item.target === wanted)
      .forEach(item => this.dismiss(item)));
  }

  private ensureContainer(): HTMLElement {
    if (!this.container) {
      this.container = document.createElement('div');
      this.container.className = 'jnks-iot-toast-stack';
      document.body.appendChild(this.container);
    }
    return this.container;
  }

  private startTimer(item: StackedToast) {
    if (item.total <= 0) {
      return;
    }
    item.remaining = item.total;
    item.deadline = Date.now() + item.remaining;
    item.timer = setTimeout(() => this.dismiss(item), item.remaining);
  }

  // 鼠标移上去就停表：读长错误信息时不该被抽走
  private pause(item: StackedToast) {
    if (item.dismissed || item.paused || item.total <= 0) {
      return;
    }
    item.paused = true;
    item.remaining = Math.max(0, item.deadline - Date.now());
    if (item.timer) {
      clearTimeout(item.timer);
      item.timer = null;
    }
  }

  private resume(item: StackedToast) {
    if (item.dismissed || !item.paused) {
      return;
    }
    item.paused = false;
    if (item.remaining > 0) {
      item.deadline = Date.now() + item.remaining;
      item.timer = setTimeout(() => this.dismiss(item), item.remaining);
    }
  }

  private dismiss(item: StackedToast) {
    if (item.dismissed) {
      return;
    }
    item.dismissed = true;
    if (item.timer) {
      clearTimeout(item.timer);
      item.timer = null;
    }
    // 只起关闭动画，真正的销毁在动画结束后回调进来
    item.ref.instance.dismiss();
    // 兜底：入场动画还没跑完就被撤掉时关闭动画不一定回调，别把卡片永久留在屏幕上
    item.killTimer = setTimeout(() => this.remove(item), 800);
  }

  private remove(item: StackedToast) {
    if (item.removed) {
      return;
    }
    item.removed = true;
    if (item.killTimer) {
      clearTimeout(item.killTimer);
      item.killTimer = null;
    }
    const index = this.items.indexOf(item);
    if (index >= 0) {
      this.items.splice(index, 1);
    }
    this.appRef.detachView(item.ref.hostView);
    item.ref.destroy();
    item.slot.remove();
    if (!this.items.length && this.container) {
      this.container.remove();
      this.container = null;
    }
  }
}

// 订阅点：模板里挂 [jnks-iot-toast] 的元素只负责把通知转给上面那份堆叠
@Directive({
  selector: '[jnks-iot-toast]'
})
export class ToastDirective implements AfterViewInit, OnDestroy {

  private notificationSubscription: Subscription = null;
  private hideNotificationSubscription: Subscription = null;

  constructor(private notificationService: ToastNotificationService,
              private ngZone: NgZone,
              private appRef: ApplicationRef,
              private envInjector: EnvironmentInjector) {
  }

  ngAfterViewInit(): void {
    this.notificationSubscription = this.notificationService.getNotification().subscribe(
      (notificationMessage) => {
        if (notificationMessage && notificationMessage.message) {
          this.stack().show(notificationMessage);
        }
      }
    );

    this.hideNotificationSubscription = this.notificationService.getHideNotification().subscribe(
      (hideNotification) => {
        if (hideNotification) {
          this.ngZone.run(() => this.stack().hide(hideNotification.target));
        }
      }
    );
  }

  private stack(): ToastStack {
    if (!toastStack) {
      toastStack = new ToastStack(this.appRef, this.envInjector, this.ngZone);
    }
    return toastStack;
  }

  ngOnDestroy(): void {
    if (this.notificationSubscription) {
      this.notificationSubscription.unsubscribe();
    }
    if (this.hideNotificationSubscription) {
      this.hideNotificationSubscription.unsubscribe();
    }
  }
}

export const toastAnimations: {
  readonly showHideToast: AnimationTriggerMetadata;
} = {
  showHideToast: trigger('showHideAnimation', [
    // height: '*' 让「关闭」能把卡片高度收掉，下面的卡片顺势上移，而不是硬跳一下
    state('opened', style({height: '*', opacity: 1, transform: 'none'})),
    state('closing', style({height: 0, opacity: 0, transform: 'translateY(4px)'})),
    transition('void => opened', [
      style({height: 0, opacity: 0, transform: 'translateY(8px)'}),
      animate('{{ open }}ms cubic-bezier(0.2, 0.9, 0.3, 1)')
    ]),
    transition('opened => closing', animate('{{ close }}ms ease')),
  ]),
};

export type ToastAnimationState = 'opened' | 'closing';

@Component({
  selector: 'jnks-iot-snack-bar-component',
  templateUrl: 'snack-bar-component.html',
  styleUrls: ['snack-bar-component.scss'],
  animations: [toastAnimations.showHideToast]
})
export class JnksIotSnackBarComponent implements OnDestroy {

  @HostBinding('class')
  get hostClass(): string[] {
    return ['jnks-iot-toast-slot'];
  }

  public notification: NotificationMessage;
  public progressMs = 0;

  // 点整条提示即复制它的文字（错误信息通常要贴到别处去查），复制后短暂显示「已复制」
  public copied = false;
  public paused = false;
  private copiedTimeout: Timeout = null;

  animationState: ToastAnimationState = 'opened';

  animationParams = {
    open: 160,
    close: 130
  };

  constructor(@Inject(TOAST_DATA)
              private data: ToastData,
              private elementRef: ElementRef,
              private clipboard: Clipboard) {
    this.notification = data.notification;
    this.progressMs = data.progressMs || 0;
  }

  // 悬停暂停倒计时（指针落在卡片上时堆叠那边的定时器也会停表）
  @HostListener('mouseenter')
  onMouseEnter(): void {
    this.setPaused(true);
  }

  @HostListener('mouseleave')
  onMouseLeave(): void {
    this.setPaused(false);
  }

  private setPaused(paused: boolean): void {
    if (this.paused === paused) {
      return;
    }
    this.paused = paused;
    if (this.data.onHover) {
      this.data.onHover(paused);
    }
  }

  dismiss(): void {
    if (this.animationState === 'closing') {
      return;
    }
    this.animationState = 'closing';
  }

  copyMessage(): void {
    const textEl: HTMLElement = this.elementRef.nativeElement.querySelector('.toast-text');
    const text = (textEl ? textEl.innerText : this.notification.message || '').trim();
    if (text && this.clipboard.copy(text)) {
      this.copied = true;
      if (this.copiedTimeout !== null) {
        clearTimeout(this.copiedTimeout);
      }
      this.copiedTimeout = setTimeout(() => {
        this.copied = false;
        this.copiedTimeout = null;
      }, 1500);
    }
  }

  action(event: MouseEvent): void {
    event.stopPropagation();
    this.dismiss();
  }

  onHideFinished(event: AnimationEvent) {
    const { toState } = event;
    const isFadeOut = (toState as ToastAnimationState) === 'closing';
    const itFinished = this.animationState === 'closing';
    if (isFadeOut && itFinished) {
      this.data.destroyToastComponent();
    }
  }

  ngOnDestroy(): void {
    if (this.copiedTimeout !== null) {
      clearTimeout(this.copiedTimeout);
      this.copiedTimeout = null;
    }
  }
}
