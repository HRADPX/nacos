/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.consistency.ephemeral.distro.v2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncData;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncDatumSnapshot;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstanceData;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

/**
 * Distro processor for v2.
 *
 * @author xiweng.yy
 */
public class DistroClientDataProcessor extends SmartSubscriber implements DistroDataStorage, DistroDataProcessor {
    
    public static final String TYPE = "Nacos:Naming:v2:ClientData";
    
    private final ClientManager clientManager;
    
    private final DistroProtocol distroProtocol;
    
    private volatile boolean isFinishInitial;
    
    public DistroClientDataProcessor(ClientManager clientManager, DistroProtocol distroProtocol) {
        this.clientManager = clientManager;
        this.distroProtocol = distroProtocol;
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    
    @Override
    public void finishInitial() {
        isFinishInitial = true;
    }
    
    @Override
    public boolean isFinishInitial() {
        return isFinishInitial;
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientEvent.ClientChangedEvent.class);
        result.add(ClientEvent.ClientDisconnectEvent.class);
        result.add(ClientEvent.ClientVerifyFailedEvent.class);
        return result;
    }
    
    @Override
    public void onEvent(Event event) {
        if (EnvUtil.getStandaloneMode()) {
            return;
        }
        // 认证失败
        if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
            // 重新同步认证失败的节点数据到当前节点
            syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);
        } else {
            syncToAllServer((ClientEvent) event);
        }
    }
    
    private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
        // 当前节点的客户端
        Client client = clientManager.getClient(event.getClientId());
        if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
            return;
        }
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        // Verify failed data should be sync directly.
        // 认证失败的数据需要立刻同步
        // 从当前节点（与 clientId 直连的节点）获取最新的数据，封装请求，发送给需要同步的数据的节点（targetServer）
        // targetServer 对应的节点收到最新数据后，同步最新的数据到自己的数据中，来保证节点间数据一致（这是一个典型的 cp 模型）
        distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
    }
    
    private void syncToAllServer(ClientEvent event) {
        Client client = event.getClient();
        // Only ephemeral data sync by Distro, persist client should sync by raft.
        // 只有与服务端连接的客户端连接才会响应事件，如果是服务端之间数据同步的连接，不满足
        // clientManager.isResponsibleClient(client) ，即不会响应事件，避免集群数据同步请求风暴。
        if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
            return;
        }
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            // 这里用了延迟调度引擎进行调度，链路比较长，最终的 处理逻辑是 DistroDelayTaskProcessor#process，
            // 封装 DistroDataRequest 请求并通过 grpc 发送给集群的其他服务节点，DistroDataRequestHandler
            // 处理请求，最终调用该类的 processData() 方法，将下线的客户端的相关信息移除。
            distroProtocol.sync(distroKey, DataOperation.DELETE);
        } else if (event instanceof ClientEvent.ClientChangedEvent) {
            // 数据同步
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            distroProtocol.sync(distroKey, DataOperation.CHANGE);
        }
    }
    
    @Override
    public String processType() {
        return TYPE;
    }
    
    @Override
    public boolean processData(DistroData distroData) {
        switch (distroData.getType()) {
            case ADD:
            case CHANGE:
                ClientSyncData clientSyncData = ApplicationUtils.getBean(Serializer.class)
                        .deserialize(distroData.getContent(), ClientSyncData.class);
                handlerClientSyncData(clientSyncData);
                return true;
            case DELETE:
                String deleteClientId = distroData.getDistroKey().getResourceKey();
                Loggers.DISTRO.info("[Client-Delete] Received distro client sync data {}", deleteClientId);
                clientManager.clientDisconnected(deleteClientId);
                return true;
            default:
                return false;
        }
    }
    
    private void handlerClientSyncData(ClientSyncData clientSyncData) {
        Loggers.DISTRO
                .info("[Client-Add] Received distro client sync data {}, revision={}", clientSyncData.getClientId(),
                        clientSyncData.getAttributes().getClientAttribute(ClientConstants.REVISION, 0L));
        // 根据 clientId 创建 Client 对象到当前节点的 ClientManager 中，这里调用的是 syncClientConnected 方法，
        // ConnectionBasedClient 对象的 isNative = false，这样注册的完成后，发送的客户端变更事件（ClientChangedEvent）
        // 就不会同步给集群了，因为这是同步连接。
        // @see #onEvent
        clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes());
        Client client = clientManager.getClient(clientSyncData.getClientId());
        // 异步事件机制完成数据注册订阅信息的同步
        upgradeClient(client, clientSyncData);
    }

    /**
     * @param client 当前服务节点的 client，如果是数据同步的话，可能是空的，如新节点加入集群
     */
    private void upgradeClient(Client client, ClientSyncData clientSyncData) {
        Set<Service> syncedService = new HashSet<>();
        // process batch instance sync logic
        // 处理 batch instance 同步逻辑
        processBatchInstanceDistroData(syncedService, client, clientSyncData);
        List<String> namespaces = clientSyncData.getNamespaces();
        List<String> groupNames = clientSyncData.getGroupNames();
        List<String> serviceNames = clientSyncData.getServiceNames();
        // 所有的客户端实例，内部包含 ip 和端口信息
        List<InstancePublishInfo> instances = clientSyncData.getInstancePublishInfos();
        
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            InstancePublishInfo instancePublishInfo = instances.get(i);
            if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) {
                // 加 client
                client.addServiceInstance(singleton, instancePublishInfo);
                // 后续逻辑同注册逻辑，唯一不同的是注册完成后不会向集群其他节点同步了...
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
        // 移除已经下线的服务
        for (Service each : client.getAllPublishedService()) {
            if (!syncedService.contains(each)) {
                client.removeServiceInstance(each);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientDeregisterServiceEvent(each, client.getClientId()));
            }
        }
    }
    
    private static void processBatchInstanceDistroData(Set<Service> syncedService, Client client,
            ClientSyncData clientSyncData) {
        BatchInstanceData batchInstanceData = clientSyncData.getBatchInstanceData();
        if (batchInstanceData == null || CollectionUtils.isEmpty(batchInstanceData.getNamespaces())) {
            Loggers.DISTRO.info("[processBatchInstanceDistroData] BatchInstanceData is null , clientId is :{}",
                    client.getClientId());
            return;
        }
        List<String> namespaces = batchInstanceData.getNamespaces();
        List<String> groupNames = batchInstanceData.getGroupNames();
        List<String> serviceNames = batchInstanceData.getServiceNames();
        List<BatchInstancePublishInfo> batchInstancePublishInfos = batchInstanceData.getBatchInstancePublishInfos();
        
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            BatchInstancePublishInfo batchInstancePublishInfo = batchInstancePublishInfos.get(i);
            BatchInstancePublishInfo targetInstanceInfo = (BatchInstancePublishInfo) client
                    .getInstancePublishInfo(singleton);
            boolean result = false;
            if (targetInstanceInfo != null) {
                result = batchInstancePublishInfo.equals(targetInstanceInfo);
            }
            if (!result) {
                client.addServiceInstance(service, batchInstancePublishInfo);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
        client.setRevision(clientSyncData.getAttributes().<Integer>getClientAttribute(ClientConstants.REVISION, 0));
    }
    
    @Override
    public boolean processVerifyData(DistroData distroData, String sourceAddress) {
        DistroClientVerifyInfo verifyData = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
        if (clientManager.verifyClient(verifyData)) {
            return true;
        }
        Loggers.DISTRO.info("client {} is invalid, get new client from {}", verifyData.getClientId(), sourceAddress);
        return false;
    }
    
    @Override
    public boolean processSnapshot(DistroData distroData) {
        // 反序列化响应数据
        ClientSyncDatumSnapshot snapshot = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
        // 包含了所有的客户端连接
        for (ClientSyncData each : snapshot.getClientSyncDataList()) {
            handlerClientSyncData(each);
        }
        return true;
    }
    
    @Override
    public DistroData getDistroData(DistroKey distroKey) {
        Client client = clientManager.getClient(distroKey.getResourceKey());
        if (null == client) {
            return null;
        }
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(client.generateSyncData());
        return new DistroData(distroKey, data);
    }
    
    @Override
    public DistroData getDatumSnapshot() {
        List<ClientSyncData> datum = new LinkedList<>();
        // clientManager 保存了所有客户端连接
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            // 构造快照数据，只是 ClientManager 里的数据，不含注册的表的信息
            datum.add(client.generateSyncData());
        }
        ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
        snapshot.setClientSyncDataList(datum);
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(snapshot);
        // 封装结果
        return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
    }
    
    @Override
    public List<DistroData> getVerifyData() {
        List<DistroData> result = null;
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            // 这里的判断是看当前的客户端连接是不是与客户端直连的连接（当前节点负责的连接），如果是同步数据的连接则跳过
            if (clientManager.isResponsibleClient(client)) {
                DistroClientVerifyInfo verifyData = new DistroClientVerifyInfo(client.getClientId(),
                        client.getRevision());
                DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
                DistroData data = new DistroData(distroKey,
                        ApplicationUtils.getBean(Serializer.class).serialize(verifyData));
                data.setType(DataOperation.VERIFY);
                if (result == null) {
                    result = new LinkedList<>();
                }
                result.add(data);
            }
        }
        return result;
    }
}
