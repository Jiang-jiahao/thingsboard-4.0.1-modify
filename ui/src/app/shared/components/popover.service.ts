import {
  ComponentRef,
  ElementRef,
  Inject,
  Injectable,
  Injector,
  Renderer2,
  Type,
  ViewContainerRef
} from '@angular/core';
import {
  defaultPopoverConfig,
  DisplayPopoverConfig,
  DisplayPopoverWithComponentRefConfig,
  PopoverPreferredPlacement,
  PopoverWithTrigger
} from '@shared/components/popover.models';
import { JnksIotPopoverComponent } from '@shared/components/popover.component';
import { ComponentType } from '@angular/cdk/portal';
import { HELP_MARKDOWN_COMPONENT_TOKEN } from '@shared/components/tokens';
import { CdkOverlayOrigin } from '@angular/cdk/overlay';
import { Observable } from 'rxjs';
import { mergeDeep } from '@core/utils';

@Injectable()
export class JnksIotPopoverService {

  private popoverWithTriggers: PopoverWithTrigger[] = [];

  constructor(@Inject(HELP_MARKDOWN_COMPONENT_TOKEN) private helpMarkdownComponent: ComponentType<any>) {
  }

  hasPopover(trigger: Element): boolean {
    const res = this.findPopoverByTrigger(trigger);
    return res !== null;
  }

  hidePopover(trigger: Element): boolean {
    const component: JnksIotPopoverComponent = this.findPopoverByTrigger(trigger);
    if (component && component.jnksIotVisible) {
      component.hide();
      return true;
    } else {
      return false;
    }
  }

  createPopoverRef(hostView: ViewContainerRef): ComponentRef<JnksIotPopoverComponent> {
    return hostView.createComponent(JnksIotPopoverComponent);
  }

  displayPopover<T>(config: DisplayPopoverConfig<T>): JnksIotPopoverComponent<T>;
  displayPopover<T>(trigger: Element, renderer: Renderer2, hostView: ViewContainerRef,
                    componentType: Type<T>, preferredPlacement: PopoverPreferredPlacement,
                    hideOnClickOutside: boolean, injector?: Injector, context?: any, overlayStyle?: any,
                    popoverStyle?: any, style?: any,
                    showCloseButton?: boolean, visibleFn?: (visible: boolean) => void,
                    popoverContentStyle?: any): JnksIotPopoverComponent<T>;
  displayPopover<T>(config: Element | DisplayPopoverConfig<T>, renderer?: Renderer2, hostView?: ViewContainerRef,
                    componentType?: Type<T>, preferredPlacement?: PopoverPreferredPlacement,
                    hideOnClickOutside?: boolean, injector?: Injector, context?: any, overlayStyle?: any,
                    popoverStyle?: any, style?: any,
                    showCloseButton?: boolean, visibleFn?: (visible: boolean) => void,
                    popoverContentStyle?: any): JnksIotPopoverComponent<T> {
    if (!(config instanceof Element) && 'trigger' in config && 'renderer' in config && 'componentType' in config) {
      const componentRef = this.createPopoverRef(config.hostView);
      return this.displayPopoverWithComponentRef<T>({ ...config, componentRef })
    } else if (config instanceof Element) {
      const componentRef = this.createPopoverRef(hostView);
      return this.displayPopoverWithComponentRef<T>(componentRef, config, renderer, componentType, preferredPlacement, hideOnClickOutside,
        injector, context, overlayStyle, popoverStyle, style, showCloseButton, visibleFn, popoverContentStyle);
    } else {
      throw new Error("Invalid configuration provided for displayPopover");
    }
  }

  displayPopoverWithComponentRef<T>(config: DisplayPopoverWithComponentRefConfig<T>): JnksIotPopoverComponent<T>;
  displayPopoverWithComponentRef<T>(componentRef: ComponentRef<JnksIotPopoverComponent>, trigger: Element, renderer: Renderer2,
                                    componentType: Type<T>, preferredPlacement: PopoverPreferredPlacement,
                                    hideOnClickOutside: boolean, injector?: Injector, context?: any, overlayStyle?: any,
                                    popoverStyle?: any, style?: any, showCloseButton?: boolean,
                                    visibleFn?: (visible: boolean) => void, popoverContentStyle?: any): JnksIotPopoverComponent<T>;
  displayPopoverWithComponentRef<T>(config: ComponentRef<JnksIotPopoverComponent> | DisplayPopoverWithComponentRefConfig<T>,
                                    trigger?: Element, renderer?: Renderer2, componentType?: Type<T>,
                                    preferredPlacement?: PopoverPreferredPlacement, hideOnClickOutside?: boolean,
                                    injector?: Injector, context?: any, overlayStyle?: any,
                                    popoverStyle?: any, style?: any, showCloseButton?: boolean,
                                    visibleFn?: (visible: boolean) => void,
                                    popoverContentStyle: any = {}): JnksIotPopoverComponent<T> {
    let popoverConfig: DisplayPopoverWithComponentRefConfig<T>;
    if (!(config instanceof ComponentRef) && 'trigger' in config && 'renderer' in config && 'componentType' in config) {
      popoverConfig = config;
    } else if(config instanceof ComponentRef) {
      popoverConfig = {
        componentRef: config,
        trigger,
        renderer,
        componentType,
        preferredPlacement,
        hideOnClickOutside,
        injector,
        context,
        overlayStyle,
        popoverStyle,
        style,
        showCloseButton,
        visibleFn,
        popoverContentStyle
      }
    } else {
      throw new Error("Invalid configuration provided for displayPopoverWithComponentRef");
    }
    popoverConfig = mergeDeep({} as any, defaultPopoverConfig, popoverConfig);
    return this._displayPopoverWithComponentRef(popoverConfig);
  }


  private _displayPopoverWithComponentRef<T>(conf: DisplayPopoverWithComponentRefConfig<T>): JnksIotPopoverComponent<T> {
    const component = conf.componentRef.instance;
    this.popoverWithTriggers.push({
      trigger: conf.trigger,
      popoverComponent: component
    });
    conf.renderer.removeChild(
      conf.renderer.parentNode(conf.trigger),
      conf.componentRef.location.nativeElement
    );
    const originElementRef = new ElementRef(conf.trigger);
    component.setOverlayOrigin(new CdkOverlayOrigin(originElementRef));
    component.jnksIotPlacement = conf.preferredPlacement;
    component.jnksIotComponent = conf.componentType;
    component.jnksIotComponentInjector = conf.injector;
    component.jnksIotComponentContext = conf.context;
    component.jnksIotOverlayStyle = conf.overlayStyle;
    component.jnksIotModal = conf.isModal;
    component.jnksIotPopoverInnerStyle = conf.popoverStyle;
    component.jnksIotPopoverInnerContentStyle = conf.popoverContentStyle;
    component.jnksIotComponentStyle = conf.style;
    component.jnksIotHideOnClickOutside = conf.hideOnClickOutside;
    component.jnksIotShowCloseButton = conf.showCloseButton;
    component.jnksIotVisibleChange.subscribe((visible: boolean) => {
      if (!visible) {
        conf.componentRef.destroy();
      }
    });
    component.jnksIotDestroy.subscribe(() => {
      this.removePopoverByComponent(component);
    });
    component.jnksIotHideStart.subscribe(() => {
      conf.visibleFn(false);
    });
    component.show();
    conf.visibleFn(true);
    return component;
  }

  toggleHelpPopover(trigger: Element, renderer: Renderer2, hostView: ViewContainerRef, helpId = '',
                    helpContent = '',
                    helpContentBase64 = '',
                    asyncHelpContent: Observable<string> = null,
                    visibleFn: (visible: boolean) => void = () => {},
                    readyFn: (ready: boolean) => void = () => {},
                    preferredPlacement: PopoverPreferredPlacement = 'bottom',
                    overlayStyle: any = {}, helpStyle: any = {}) {
    if (this.hasPopover(trigger)) {
      this.hidePopover(trigger);
    } else {
      readyFn(false);
      const injector = Injector.create({
        parent: hostView.injector, providers: []
      });
      const componentRef = hostView.createComponent(JnksIotPopoverComponent);
      const component = componentRef.instance;
      this.popoverWithTriggers.push({
        trigger,
        popoverComponent: component
      });
      renderer.removeChild(
        renderer.parentNode(trigger),
        componentRef.location.nativeElement
      );
      const originElementRef = new ElementRef(trigger);
      component.jnksIotAnimationState = 'void';
      component.jnksIotOverlayStyle = {...overlayStyle, opacity: '0' };
      component.setOverlayOrigin(new CdkOverlayOrigin(originElementRef));
      component.jnksIotPlacement = preferredPlacement;
      component.jnksIotComponent = this.helpMarkdownComponent;
      component.jnksIotComponentInjector = injector;
      component.jnksIotComponentContext = {
        helpId,
        helpContent,
        helpContentBase64,
        asyncHelpContent,
        style: helpStyle,
        visible: true
      };
      component.jnksIotHideOnClickOutside = true;
      component.jnksIotVisibleChange.subscribe((visible: boolean) => {
        if (!visible) {
          visibleFn(false);
          componentRef.destroy();
        }
      });
      component.jnksIotDestroy.subscribe(() => {
        this.removePopoverByComponent(component);
      });
      const showHelpMarkdownComponent = () => {
        component.jnksIotOverlayStyle = {...component.jnksIotOverlayStyle, opacity: '1' };
        component.jnksIotAnimationState = 'active';
        component.updatePosition();
        readyFn(true);
        setTimeout(() => {
          component.updatePosition();
        });
      };
      const setupHelpMarkdownComponent = (helpMarkdownComponent: any) => {
        if (helpMarkdownComponent.isMarkdownReady) {
          showHelpMarkdownComponent();
        } else {
          helpMarkdownComponent.markdownReady.subscribe(() => {
            showHelpMarkdownComponent();
          });
        }
      };
      if (component.jnksIotComponentRef) {
        setupHelpMarkdownComponent(component.jnksIotComponentRef.instance);
      } else {
        component.jnksIotComponentChange.subscribe((helpMarkdownComponentRef) => {
          setupHelpMarkdownComponent(helpMarkdownComponentRef.instance);
        });
      }
      component.show();
      visibleFn(true);
    }
  }

  private findPopoverByTrigger(trigger: Element): JnksIotPopoverComponent | null {
    const res = this.popoverWithTriggers.find(val => this.elementsAreEqualOrDescendant(trigger, val.trigger));
    if (res) {
      return res.popoverComponent;
    } else {
      return null;
    }
  }

  private removePopoverByComponent(component: JnksIotPopoverComponent): void {
    const index = this.popoverWithTriggers.findIndex(val => val.popoverComponent === component);
    if (index > -1) {
      this.popoverWithTriggers.splice(index, 1);
    }
  }

  private elementsAreEqualOrDescendant(element1: Element, element2: Element): boolean {
    return element1 === element2 || element1.contains(element2) || element2.contains(element1);
  }
}
