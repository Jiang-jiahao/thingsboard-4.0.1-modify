/**
 * Copyright 濠曪拷 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.queue.processing;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.AssetProfileId;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.TenantProfileId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.msg.plugin.ComponentLifecycleMsg;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.QueueConsumerManager;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldCache;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.queue.TbPackCallback;
import org.thingsboard.server.service.queue.TbPackProcessingContext;
import org.thingsboard.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 婵炴垵鐗愰崹鍌炴嚀閸涱喗绠涢柛鏃撶磿濞堟垿宕洪搹纭咁潶
 * @param <N>
 */
@RequiredArgsConstructor
public abstract class AbstractConsumerService<N extends com.google.protobuf.GeneratedMessageV3> extends TbApplicationEventListener<PartitionChangeEvent> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    // Actor缂侇垵宕电划鐑樼▔婵犱胶鐟撻柡鍌︽嫹
    protected final ActorSystemContext actorContext;

    // 缂佸鍠愰崺娑㈡煀瀹ュ洨鏋傜紓鍌涙尭閻★拷
    protected final TbTenantProfileCache tenantProfileCache;

    // 閻犱焦鍎抽ˇ顒勬煀瀹ュ洨鏋傜紓鍌涙尭閻★拷
    protected final TbDeviceProfileCache deviceProfileCache;

    // 閻犙冨妤犲洭鏌婂鍥╂瀭缂傚倹鎸搁悺锟�
    protected final TbAssetProfileCache assetProfileCache;

    // 閻犱緤绱曢悾鑽も偓娑欘殕椤斿瞼绱撻幘宕囨憼
    protected final CalculatedFieldCache calculatedFieldCache;

    // API濞达綀娉曢弫銈夋偐閼哥鍋撴担瑙勭疀闁告棑鎷�
    protected final TbApiUsageStateService apiUsageStateService;

    // 闁告帒妫楃亸顖滅不閿涘嫭鍊為柡鍫濈Т婵拷
    protected final PartitionService partitionService;

    // 閹煎瓨姊婚弫銈嗙鐎ｂ晜顐介柛娆愬灥缁旂兘宕抽敓锟�
    protected final ApplicationEventPublisher eventPublisher;

    protected final Optional<JwtSettingsService> jwtSettingsService;

    // 闂侇偅姘ㄩ悡鈥斥槈閸喍绱栨繛鎴濈墣閸ㄥ倿鎳撻崨顖ｅ悁闁荤偛妫楀▍锟�
    protected QueueConsumerManager<TbProtoQueueMsg<N>> nfConsumer;

    // 婵炴垵鐗愰崹鍌炴嚀閸涱垰娈犵紒瀣儐閻粓鏁嶉崼婵喰楅柟顑跨閵囧洨浜歌箛銉х
    protected ExecutorService consumersExecutor;

    // 缂佺媴绱曢幃濠勭棯鐠恒劉鏌ゆ慨鍦缁辨瑩宕堕崫鍕毎濠㈠爢鍐瘓闁挎冻鎷�
    protected ExecutorService mgmtExecutor;

    // 閻犲鍟€瑰磭鐥捄銊㈡煠婵湱濯寸槐娆撳础閺囩姴娈犵紒瀣儜缁憋拷
    protected ScheduledExecutorService scheduler;

    /**
     * 闁哄牆绉存慨鐔煎礆濠靛棭娼楅柛鏍ㄧ壄缁辨瑩妫侀埀顒勫捶閵娿儳鎽嶇紒顐ょ帛閻庮垶鏌呴悩鍙夊€甸悹瀣暟閺併倝鏁嶉敓锟�
     * @param prefix 缂佹崘娉曢埢濂稿触瀹ュ懎顤呯紓鍌楀亾闁挎稑鐗忛弫銈嗙鎼淬垻鍨奸悹鍥ф濠€鍥礉閿涘嫯顫﹂柛銊ヮ儜缁憋拷
     */
    public void init(String prefix) {
        // 闁告帗绋戠紓鎾剁棯鐠恒劉鏌ゆ慨鍦缁辨瑩宕ㄩ挊澶嬪€抽悷娆忓鐎垫牠鏁嶉敓锟�<prefix>-<role>闁挎冻鎷�
        this.consumersExecutor = Executors.newCachedThreadPool(ThingsBoardThreadFactory.forName(prefix + "-consumer"));
        this.mgmtExecutor = ThingsBoardExecutors.newWorkStealingPool(getMgmtThreadPoolSize(), prefix + "-mgmt");
        this.scheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor(prefix + "-consumer-scheduler");

        // 闁哄秷顫夊畵浣衡偓娑欏姉鐞氼偊寮靛鍛潳缂侇偉顕ч悗鐑藉级閵夛妇鈧垰顕欏ú顏佸亾濮樿京鍙€婵炴垵鐗愰崹鍌炴嚀閸涱垼鍚€闁荤偛妫楀▍锟�
        this.nfConsumer = QueueConsumerManager.<TbProtoQueueMsg<N>>builder()
                .name(getServiceType().getLabel() + " Notifications")
                .msgPackProcessor(this::processNotifications) // 缂備焦鍨甸悾鎯р槈閸喍绱栧璺哄閹﹪宕欓懞銉︽
                .pollInterval(getNotificationPollDuration()) // 闂佹澘绉堕悿鍡樻姜椤旀鍤勯梻鍌涙尦濞堬拷
                .consumerCreator(this::createNotificationsConsumer) // 缂備焦鍨甸悾鎯р槈閸絽鐎柤鏉挎噹閸ㄥ崬顕欓崫鍕彜
                .consumerExecutor(consumersExecutor) // 闁圭ǹ娲ら悾楣冨箥瑜戦、鎴犵棯鐠恒劉鏌ゆ慨鐧告嫹
                .threadPrefix("notifications") // 闁告劕鎳橀崕瀵哥棯鐠恒劉鏌ら柛鎾崇Ф缁憋拷
                .build();
    }

    /**
     * 缂侇垵宕电划娲触椤栨艾袟闁告艾楠搁崹鍨叏鐎ｎ亜顕ф繛鎴濈墣閸ㄥ倿鎳撻崪鍐闂侇偅淇虹换鍎傽link AfterStartUp}婵炲鍔忚闁硅矇鍐ㄧ厬闁圭瑳鍡╂斀濡炪倕鎼花顓㈡晬閿燂拷
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    public void afterStartUp() {
        startConsumers();
    }

    /**
     * 闁告凹鍨版慨鈺呭箥閳ь剟寮垫径瀣ラ悹鎰攰閳ь剨鎷�
     */
    protected void startConsumers() {
        // 閻犱降鍨藉Σ鍕槈閸喍绱栧☉鎾愁煼椤ｏ拷
        nfConsumer.subscribe();
        // 闁告凹鍨版慨鈺佲槈閸絽鐎紒鎹愭硶閳伙拷
        nfConsumer.launch();
    }

    /**
     * 闁告帒妫楃亸顖炲矗濡粯绾ù婊冾儎濞嗐垺娼婚崶銊﹀Б闁挎稑鐗呯划搴㈠緞閸曨厽鍊為柡鍫墯濠€鍥礉閿涘嫯顫﹂柛銊ヮ儑濞堟垿宕ｅΟ缁樼函闁挎冻鎷�
     */
    @Override
    protected boolean filterTbApplicationEvent(PartitionChangeEvent event) {
        return event.getServiceType() == getServiceType();
    }

    protected abstract ServiceType getServiceType();

    protected void stopConsumers() {
        nfConsumer.stop();
    }

    protected abstract long getNotificationPollDuration();

    protected abstract long getNotificationPackProcessingTimeout();

    protected abstract int getMgmtThreadPoolSize();

    protected abstract TbQueueConsumer<TbProtoQueueMsg<N>> createNotificationsConsumer();

    /**
     * 濠㈣泛瀚幃濠囨焻濮樿京鍙€婵炴垵鐗婃导鍛村礌閸滃啰绀勯柡宥嚽圭缓鎯规担琛℃煠闁挎冻鎷�
     * @param msgs 闁告ḿ鍠庨～鎰槈閸喍绱栭柛鎺擃殙閵嗭拷
     * @param consumer 婵炴垵鐗愰崹鍌炴嚀閸涱厾鏉藉〒姘儜缁辨瑩鎮介妸銈囪壘闁圭粯鍔掑锕傚磻韫囨泤鈺呮煂韫囥儳绀�
     */
    protected void processNotifications(List<TbProtoQueueMsg<N>> msgs, TbQueueConsumer<TbProtoQueueMsg<N>> consumer) throws Exception {
        // 1. 婵炴垵鐗婃导鍛村礌閸涢偊妫呴柨娑樼墔鐠愮喎袙韫囨梹钂嬫繛鎴濈墛娴煎懘宕氶崱娑樺赋闁哥儐鍨粩纰擠闁挎冻鎷�
        List<IdMsgPair<N>> orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).toList();
        ConcurrentMap<UUID, TbProtoQueueMsg<N>> pendingMap = orderedMsgList.stream().collect(
                Collectors.toConcurrentMap(IdMsgPair::getUuid, IdMsgPair::getMsg));
        CountDownLatch processingTimeoutLatch = new CountDownLatch(1);
        // 2. 闁哄瀚紓鎾村緞閸曨厽鍊炲☉鎾筹梗缁楀懘寮敓锟�
        TbPackProcessingContext<TbProtoQueueMsg<N>> ctx = new TbPackProcessingContext<>(
                processingTimeoutLatch, pendingMap, new ConcurrentHashMap<>());
        // 3. 妤犵偞鍎奸、鎴炲緞閸曨厽鍊炴繛鎴濈墛娴硷拷
        orderedMsgList.forEach(element -> {
            UUID id = element.getUuid();
            TbProtoQueueMsg<N> msg = element.getMsg();
            log.trace("[{}] Creating notification callback for message: {}", id, msg.getValue());
            TbCallback callback = new TbPackCallback<>(id, ctx);
            try {
                handleNotification(id, msg, callback);
            } catch (Throwable e) {
                log.warn("[{}] Failed to process notification: {}", id, msg, e);
                callback.onFailure(e);
            }
        });
        // 4. 缂佹稑顦欢鐔稿緞閸曨厽鍊為悗鐟版湰閸ㄦ岸鏁嶉崼锝囆㈤柡鍐煐濠р偓闁告帟顔愮槐锟�
        if (!processingTimeoutLatch.await(getNotificationPackProcessingTimeout(), TimeUnit.MILLISECONDS)) {
            // 閻犱焦婢樼紞宥囨惥閸涱喗顦ф繛鎴濈墛娴硷拷
            ctx.getAckMap().forEach((id, msg) -> log.warn("[{}] Timeout to process notification: {}", id, msg.getValue()));
            ctx.getFailedMap().forEach((id, msg) -> log.warn("[{}] Failed to process notification: {}", id, msg.getValue()));
        }
        // 5. 闁圭粯鍔掑锕€鈽夐崼锝呯€柛瀣箳浜涢梺璇ф嫹
        consumer.commit();
    }

    /**
     * 濠㈣泛瀚幃濠勭磼閸曨亝顐介柣銏㈠枎閹筹繝宕ㄩ妸锔藉焸濞存粌顑勫▎銏ゆ儍閸曨剛澹嬮煫鍥у暢閻箖鎮介柆宥佸亾閺勫繒甯�
     * 闁哄秷顫夊畵浣圭▔瀹ュ懏鍊遍柣銊ュ閻ゅ嫭鎷呴幘鎯邦潶闁搞劌顑嗘晶鐣屾偘瀹€鈧ù澶嬫償閺冨倹鐣辩紓鍌涙尭閻°劍寰勬潏銊︽珡闁告粌鐬兼慨鎼佸箑娴ｈ绾柡鍌滃閹奸攱鎷呴敓锟�
     * @param id 婵炴垵鐗婃导鍛崉閻斿鍤婭D闁挎稑鐗忛弫銈嗙鎼淬垺锛夐煫鍥殙閹风兘鐓搴ｇ
     * @param componentLifecycleMsg 闁汇垻鍠庨幊锟犲川閵婏附鍩傚ù婊冾儎濞嗐垹鈽夐崼鐔剁礀
     */
    protected final void handleComponentLifecycleMsg(UUID id, ComponentLifecycleMsg componentLifecycleMsg) {
        TenantId tenantId = componentLifecycleMsg.getTenantId();
        log.debug("[{}][{}][{}] Received Lifecycle event: {}", tenantId, componentLifecycleMsg.getEntityId().getEntityType(),
                componentLifecycleMsg.getEntityId(), componentLifecycleMsg.getEvent());
        // 闁圭ǹ顦悿鍕媴閹炬儼顫﹂柛銊ヮ儓閻箖鎮介崡鐑嗘П闁荤儑鎷�
        if (EntityType.TENANT_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 缂佸鍠愰崺娑㈡煀瀹ュ洨鏋傞柛娆惿戝ú鎸庡緞閸曨厽鍊�
            TenantProfileId tenantProfileId = new TenantProfileId(componentLifecycleMsg.getEntityId().getId());
            // 濠㈡儼椴搁弲銉х矓閻旂ǹ鐓曢梺鏉跨Ф閻ゅ棛绱撻幘宕囨憼
            tenantProfileCache.evict(tenantProfileId);
            // 濠碘€冲€归悘澶愬及椤栨粠娼抽柟瀵稿厴閸樸倗绱旈鐓庣秮闁哄洩鎻槐婵嬬嵁閼稿灚绾柡鍌滄PI濞达綀娉曢弫銈夋偐閼哥鍋撴笟濠勭闁搞儳濮崇拹鐔虹矓閻旂ǹ鐓曢梺鏉跨Ф閻ゅ棝宕€瑰窋i濞达綀娉曢弫銈夋⒔閹邦厾銈︾紒娑橆槺濞村宕楃粵鍦
            if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                apiUsageStateService.onTenantProfileUpdate(tenantProfileId);
            }
        } else if (EntityType.TENANT.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 缂佸鍠愰崺娑㈠矗濡粯绾璺哄閹拷
            if (TenantId.SYS_TENANT_ID.equals(tenantId)) {
                // 濠碘€冲€归悘澶愬矗濡粯绾柣銊ュ濡插摜鍖栭懡銈囧煚缂佸鍠愰崺娑㈡晬鐏炶棄鐏熼梺鎻掔У閺屽﹪宕濋悩鐑樼グJWT閻犱礁澧介悿锟�
                jwtSettingsService.ifPresent(JwtSettingsService::reloadJwtSettings);
                return;
            } else {
                // 闁哄拋鍣ｉ埀顒佹皑椤倝骞嬮崙銈囩獥濠㈡儼椴搁弲銉х矓閻旂ǹ鐓曠紓鍌涙尭閻°劑鐛捄鎭掍杭闁轰礁鐗嗛崹搴ㄥ礌鏉炴媽鍘柣銊ュ椤倝骞嬫搴ｅ閻庢冻鎷�
                tenantProfileCache.evict(tenantId);
                partitionService.evictTenantInfo(tenantId);
                if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.UPDATED)) {
                    // 濠碘€冲€归悘澶愬及椤栨粠娼抽柟鏉戝槻瑜板寮存潏鍓х闁告帗鐟﹀ú鍧楀棘閻у摉I濞达綀娉曢弫銈夋偐閼哥鍋撻敓锟�
                    apiUsageStateService.onTenantUpdate(tenantId);
                } else if (componentLifecycleMsg.getEvent().equals(ComponentLifecycleEvent.DELETED)) {
                    // 濠碘€冲€归悘澶愬及椤栨粠娼抽柟鏉戝槻閸ㄥ綊姊介妶蹇曠闁告帗鐟ラ崹褰掓⒔椤ヮ湒I濞达綀娉曢弫銈夋偐閼哥鍋撴担姝屽珯闁告帞濞€濞呭酣宕氶崱妤€闅樼紒澶屽枑閸╂稒绌遍埄鍐х礀缂傚倹鎸搁悺锟�
                    apiUsageStateService.onTenantDelete(tenantId);
                    partitionService.removeTenant(tenantId);
                }
            }
        } else if (EntityType.DEVICE_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻犱焦鍎抽ˇ顒勬煀瀹ュ洨鏋傞柛娆惿戝ú鍧楁晬濮橆兙浜奸柡浣哥墣椤旀洘寰勯崶顒€甯崇紓鍐惧枤缁憋妇鈧冻鎷�
            deviceProfileCache.evict(tenantId, new DeviceProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.DEVICE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻犱焦鍎抽ˇ顒勫矗濡粯绾柨娑欒壘閵囨垿寮崼锝庡晭濠㈣泛娲ㄥù澶愬礂瀹曞洨澶勯悗娑虫嫹
            deviceProfileCache.evict(tenantId, new DeviceId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET_PROFILE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻犙冨妤犲洭鏌婂鍥╂瀭闁告瑦蓱濞插潡鏁嶅顑句杭闁轰礁鐗愮粊顐ｇ瑜旈崢銈囩磾椤斿墽澶勯悗娑虫嫹
            assetProfileCache.evict(tenantId, new AssetProfileId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ASSET.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻犙冨妤犲洭宕ｅΟ缁樼函闁挎稒鑹鹃妵鎴﹀极閸絿銈ù婧犲懏绁查柛蹇撶－缁憋妇鈧冻鎷�
            assetProfileCache.evict(tenantId, new AssetId(componentLifecycleMsg.getEntityId().getId()));
        } else if (EntityType.ENTITY_VIEW.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻庡湱鍋樼紞瀣喆閸℃绂堥柛娆惿戝ú鍧楁晬濮樿櫕绁柛娆愬灩缁壆鈧湱鍋樼紞瀣喆閸℃绂堥柡鍫濈Т婵喐寰勯崟顓熷€�
            if (actorContext.getTbEntityViewService() != null) {
                actorContext.getTbEntityViewService().onComponentLifecycleMsg(componentLifecycleMsg);
            }
        } else if (EntityType.API_USAGE_STATE.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // API濞达綀娉曢弫銈夋偐閼哥鍋撴担绋跨秮闁哄洩鎻槐浼村即鐎涙ɑ鐓€API濞达綀娉曢弫銈夋偐閼哥鍋撻敓锟�
            apiUsageStateService.onApiUsageStateUpdate(tenantId);
        } else if (EntityType.CUSTOMER.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻庡箍鍨洪崺娑㈠礆閻樼粯鐝熼柨娑欑缁斿鎮堕崱娆愮ゲ闁稿繒鐓I濞达綀娉曢弫銈夋偐閼哥鍋撻敓锟�
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.DELETED) {
                apiUsageStateService.onCustomerDelete((CustomerId) componentLifecycleMsg.getEntityId());
            }
        } else if (EntityType.CALCULATED_FIELD.equals(componentLifecycleMsg.getEntityId().getEntityType())) {
            // 閻犱緤绱曢悾鑽も偓娑欘殕椤斿矂宕ｅΟ缁樼函闁挎稒纰嶅ú鍧楀棘閹峰矈鍚€缂佺姵顨呴悺褍鈻撻悽鐢靛閻庢冻鎷�
            if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.CREATED) {
                calculatedFieldCache.addCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else if (componentLifecycleMsg.getEvent() == ComponentLifecycleEvent.UPDATED) {
                calculatedFieldCache.updateCalculatedField(tenantId, (CalculatedFieldId) componentLifecycleMsg.getEntityId());
            } else {
                calculatedFieldCache.evict((CalculatedFieldId) componentLifecycleMsg.getEntityId());
            }
        }
        // 闁告瑦鍨电粩閿嬬鐎ｂ晜顐介柨娑樼墣琚濋柛娆愬灥閸欑偓绂掗弽顐ｇ＇闁告凹鍓欏▍鎺楁晬閿燂拷
        eventPublisher.publishEvent(componentLifecycleMsg);
        log.trace("[{}] Forwarding component lifecycle message to App Actor {}", id, componentLifecycleMsg);
        // 閺夌儐鍓欒ぐ鍌滅磼濮楁紲tor缂侇垵宕电划鐑樺緞閸曨厽鍊�
        actorContext.tellWithHighPriority(componentLifecycleMsg);
    }

    protected abstract void handleNotification(UUID id, TbProtoQueueMsg<N> msg, TbCallback callback) throws Exception;

    @PreDestroy
    public void destroy() {
        stopConsumers();
        if (consumersExecutor != null) {
            consumersExecutor.shutdownNow();
        }
        if (mgmtExecutor != null) {
            mgmtExecutor.shutdownNow();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

}
