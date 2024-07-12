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

package com.alibaba.nacos.naming.core.v2.client;

import static com.alibaba.nacos.naming.constants.ClientConstants.REVISION;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstanceData;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.utils.DistroUtils;

/**
 * Abstract implementation of {@code Client}.
 *
 * @author xiweng.yy
 */
public abstract class AbstractClient implements Client {

    // 客户端启动发起注册请求时，保存 Service 和某一个实例的映射，通常来说，一个服务（Service）会有多个实例，value 应该用集合来存储所有的实例信息。
    // 但是该对象是一个从客户端（需要注册的实例）连向服务端的实例，每个实例都有一个对应的客户端连接，保存在 ClientManager 中，
    // key 为 clientId, value 为客户端实例。当前客户端不应该维护其他实例的信息的，所以这里只是存储服务与当前实例的映射关系。
    // 问题：客户端如何获取该服务的所有实例？
    // 服务的每个客户端实例发起注册时，会向 ClientServiceIndexesManager#publisherIndexes 写入对应的客户端id，可以通过客户端 id 从 ClientManager
    // 获取所有的客户端实例，再从各个客户端实例中获取对应服务的实例信息，通过信息组装返回给客户端。
    // ServiceStorage.getPushData：组装服务的所有的实例信息。
    protected final ConcurrentHashMap<Service, InstancePublishInfo> publishers = new ConcurrentHashMap<>(16, 0.75f, 1);

    // 客户端启动发起订阅请求时，保存 Service 和订阅者的映射关系
    // 一个客户端可以订阅多个服务，如订单服务，用户服务等
    // SubscribeServiceRequestHandler#handle
    protected final ConcurrentHashMap<Service, Subscriber> subscribers = new ConcurrentHashMap<>(16, 0.75f, 1);
    
    protected volatile long lastUpdatedTime;
    
    protected final AtomicLong revision;
    
    protected ClientAttributes attributes;
    
    public AbstractClient(Long revision) {
        lastUpdatedTime = System.currentTimeMillis();
        this.revision = new AtomicLong(revision == null ? 0 : revision);
    }
    
    @Override
    public void setLastUpdatedTime() {
        this.lastUpdatedTime = System.currentTimeMillis();
    }
    
    @Override
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }
    
    @Override
    public boolean addServiceInstance(Service service, InstancePublishInfo instancePublishInfo) {
        // 内部使用一个 map 保存 service 和实例的关系
        if (null == publishers.put(service, instancePublishInfo)) {
            if (instancePublishInfo instanceof BatchInstancePublishInfo) {
                MetricsMonitor.incrementIpCountWithBatchRegister(instancePublishInfo);
            } else {
                MetricsMonitor.incrementInstanceCount();
            }
        }
        // 发布 ClientChangedEvent 事件 --> DistroClientDataProcessor#onEvent 同步实例信息到其他服务器上
        NotifyCenter.publishEvent(new ClientEvent.ClientChangedEvent(this));
        Loggers.SRV_LOG.info("Client change for service {}, {}", service, getClientId());
        return true;
    }
    
    @Override
    public InstancePublishInfo removeServiceInstance(Service service) {
        InstancePublishInfo result = publishers.remove(service);
        if (null != result) {
            if (result instanceof BatchInstancePublishInfo) {
                MetricsMonitor.decrementIpCountWithBatchRegister(result);
            } else {
                MetricsMonitor.decrementInstanceCount();
            }
            NotifyCenter.publishEvent(new ClientEvent.ClientChangedEvent(this));
        }
        Loggers.SRV_LOG.info("Client remove for service {}, {}", service, getClientId());
        return result;
    }
    
    @Override
    public InstancePublishInfo getInstancePublishInfo(Service service) {
        return publishers.get(service);
    }
    
    @Override
    public Collection<Service> getAllPublishedService() {
        return publishers.keySet();
    }
    
    @Override
    public boolean addServiceSubscriber(Service service, Subscriber subscriber) {
        if (null == subscribers.put(service, subscriber)) {
            MetricsMonitor.incrementSubscribeCount();
        }
        return true;
    }
    
    @Override
    public boolean removeServiceSubscriber(Service service) {
        if (null != subscribers.remove(service)) {
            MetricsMonitor.decrementSubscribeCount();
        }
        return true;
    }

    @Override
    public Subscriber getSubscriber(Service service) {
        return subscribers.get(service);
    }
    
    @Override
    public Collection<Service> getAllSubscribeService() {
        return subscribers.keySet();
    }
    
    @Override
    public ClientSyncData generateSyncData() {
        List<String> namespaces = new LinkedList<>();
        List<String> groupNames = new LinkedList<>();
        List<String> serviceNames = new LinkedList<>();
    
        List<String> batchNamespaces = new LinkedList<>();
        List<String> batchGroupNames = new LinkedList<>();
        List<String> batchServiceNames = new LinkedList<>();
        
        List<InstancePublishInfo> instances = new LinkedList<>();
        List<BatchInstancePublishInfo> batchInstancePublishInfos = new LinkedList<>();
        BatchInstanceData  batchInstanceData = new BatchInstanceData();
        // Service -> 客户端实例信息
        for (Map.Entry<Service, InstancePublishInfo> entry : publishers.entrySet()) {
            InstancePublishInfo instancePublishInfo = entry.getValue();
            if (instancePublishInfo instanceof BatchInstancePublishInfo) {
                BatchInstancePublishInfo batchInstance = (BatchInstancePublishInfo) instancePublishInfo;
                batchInstancePublishInfos.add(batchInstance);
                buildBatchInstanceData(batchInstanceData, batchNamespaces, batchGroupNames, batchServiceNames, entry);
                batchInstanceData.setBatchInstancePublishInfos(batchInstancePublishInfos);
            } else {
                namespaces.add(entry.getKey().getNamespace());
                groupNames.add(entry.getKey().getGroup());
                serviceNames.add(entry.getKey().getName());
                instances.add(entry.getValue());
            }
        }
        ClientSyncData data = new ClientSyncData(getClientId(), namespaces, groupNames, serviceNames, instances, batchInstanceData);
        // 将版本设置到 attributes 中
        data.getAttributes().addClientAttribute(REVISION, getRevision());
        return data;
    }
    
    private static BatchInstanceData buildBatchInstanceData(BatchInstanceData  batchInstanceData, List<String> batchNamespaces,
            List<String> batchGroupNames, List<String> batchServiceNames, Map.Entry<Service, InstancePublishInfo> entry) {
        batchNamespaces.add(entry.getKey().getNamespace());
        batchGroupNames.add(entry.getKey().getGroup());
        batchServiceNames.add(entry.getKey().getName());
        
        batchInstanceData.setNamespaces(batchNamespaces);
        batchInstanceData.setGroupNames(batchGroupNames);
        batchInstanceData.setServiceNames(batchServiceNames);
        return batchInstanceData;
    }
    
    @Override
    public void release() {
        Collection<InstancePublishInfo> instancePublishInfos = publishers.values();
        for (InstancePublishInfo instancePublishInfo : instancePublishInfos) {
            if (instancePublishInfo instanceof BatchInstancePublishInfo) {
                MetricsMonitor.decrementIpCountWithBatchRegister(instancePublishInfo);
            } else {
                MetricsMonitor.getIpCountMonitor().decrementAndGet();
            }
        }
        MetricsMonitor.getSubscriberCount().addAndGet(-1 * subscribers.size());
    }
    
    @Override
    public long recalculateRevision() {
        int hash = DistroUtils.hash(this);
        revision.set(hash);
        return hash;
    }
    
    @Override
    public long getRevision() {
        return revision.get();
    }
    
    @Override
    public void setRevision(long revision) {
        this.revision.set(revision);
    }
    
    /**
     * get client attributes.
     */
    public ClientAttributes getClientAttributes() {
        return attributes;
    }
    
    public void setAttributes(ClientAttributes attributes) {
        this.attributes = attributes;
    }
}
