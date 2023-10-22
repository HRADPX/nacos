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

package com.alibaba.nacos.naming.core.v2.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.utils.InstanceUtil;

/**
 * Service storage.
 *
 * @author xiweng.yy
 */
@Component
public class ServiceStorage {
    
    private final ClientServiceIndexesManager serviceIndexesManager;
    
    private final ClientManager clientManager;
    
    private final SwitchDomain switchDomain;
    
    private final NamingMetadataManager metadataManager;

    // 存储实例的信息
    // 写入路径:
    // 1. 客户端启动发起订阅请求 -> 服务端处理请求发布 ClientSubscribeServiceClient 事件
    // -> ClientServiceIndexesManager 处理并发布 ServiceSubscribedEvent -> NamingSubscriberServiceV2Impl
    // 将事件包装成延迟任务，通过延迟任务调度引擎分发调度，最后会调用 getPushData 方法完成客户端 Service 信息的写入。
    // 2. 客户端启动时会开启一个定时查询任务，也会调用入 getPushData 方法完成信息的写入。

    // 上述两个路径都有可能完成信息的写入，但是只有一个会写入，后续都是从该缓存中获取。

    // 上面的前提是注册请求已经处理，并且将实例的 Service 对象写入 ServiceManager#singletonRepository 对象中。
    private final ConcurrentMap<Service, ServiceInfo> serviceDataIndexes;
    
    private final ConcurrentMap<Service, Set<String>> serviceClusterIndex;
    
    public ServiceStorage(ClientServiceIndexesManager serviceIndexesManager, ClientManagerDelegate clientManager,
            SwitchDomain switchDomain, NamingMetadataManager metadataManager) {
        this.serviceIndexesManager = serviceIndexesManager;
        this.clientManager = clientManager;
        this.switchDomain = switchDomain;
        this.metadataManager = metadataManager;
        this.serviceDataIndexes = new ConcurrentHashMap<>();
        this.serviceClusterIndex = new ConcurrentHashMap<>();
    }
    
    public Set<String> getClusters(Service service) {
        return serviceClusterIndex.getOrDefault(service, new HashSet<>());
    }
    
    public ServiceInfo getData(Service service) {
        return serviceDataIndexes.containsKey(service) ? serviceDataIndexes.get(service) : getPushData(service);
    }

    // see ServiceStorage#serviceDataIndexes
    public ServiceInfo getPushData(Service service) {
        ServiceInfo result = emptyServiceInfo(service);
        if (!ServiceManager.getInstance().containSingleton(service)) {
            return result;
        }
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        // 设置所有实例
        result.setHosts(getAllInstancesFromIndex(singleton));
        // 缓存
        serviceDataIndexes.put(singleton, result);
        return result;
    }
    
    public void removeData(Service service) {
        serviceDataIndexes.remove(service);
        serviceClusterIndex.remove(service);
    }
    
    private ServiceInfo emptyServiceInfo(Service service) {
        ServiceInfo result = new ServiceInfo();
        result.setName(service.getName());
        result.setGroupName(service.getGroup());
        result.setLastRefTime(System.currentTimeMillis());
        result.setCacheMillis(switchDomain.getDefaultPushCacheMillis());
        return result;
    }

    // 获取服务的所有实例
    private List<Instance> getAllInstancesFromIndex(Service service) {
        Set<Instance> result = new HashSet<>();
        Set<String> clusters = new HashSet<>();
        // 获取所有的客户端id
        for (String each : serviceIndexesManager.getAllClientsRegisteredService(service)) {
            // 根据客户端id 获取对应实例信息
            Optional<InstancePublishInfo> instancePublishInfo = getInstanceInfo(each, service);
            if (instancePublishInfo.isPresent()) {
                InstancePublishInfo publishInfo = instancePublishInfo.get();
                //If it is a BatchInstancePublishInfo type, it will be processed manually and added to the instance list
                if (publishInfo instanceof BatchInstancePublishInfo) {
                    BatchInstancePublishInfo batchInstancePublishInfo = (BatchInstancePublishInfo) publishInfo;
                    List<Instance> batchInstance = parseBatchInstance(service, batchInstancePublishInfo, clusters);
                    result.addAll(batchInstance);
                } else {
                    Instance instance = parseInstance(service, instancePublishInfo.get());
                    result.add(instance);
                    clusters.add(instance.getClusterName());
                }
            }
        }
        // cache clusters of this service
        serviceClusterIndex.put(service, clusters);
        return new LinkedList<>(result);
    }
    
    /**
     * Parse batch instance.
     * @param service service
     * @param batchInstancePublishInfo batchInstancePublishInfo
     * @return batch instance list
     */
    private List<Instance> parseBatchInstance(Service service, BatchInstancePublishInfo batchInstancePublishInfo, Set<String> clusters) {
        List<Instance> resultInstanceList = new ArrayList<>();
        List<InstancePublishInfo> instancePublishInfos = batchInstancePublishInfo.getInstancePublishInfos();
        for (InstancePublishInfo instancePublishInfo : instancePublishInfos) {
            Instance instance = parseInstance(service, instancePublishInfo);
            resultInstanceList.add(instance);
            clusters.add(instance.getClusterName());
        }
        return resultInstanceList;
    }
    
    private Optional<InstancePublishInfo> getInstanceInfo(String clientId, Service service) {
        // 根据客户端id获取客户端实例（这里的客户端实例可以看作是一个连接实例）
        Client client = clientManager.getClient(clientId);
        if (null == client) {
            return Optional.empty();
        }
        return Optional.ofNullable(client.getInstancePublishInfo(service));
    }
    
    private Instance parseInstance(Service service, InstancePublishInfo instanceInfo) {
        Instance result = InstanceUtil.parseToApiInstance(service, instanceInfo);
        Optional<InstanceMetadata> metadata = metadataManager
                .getInstanceMetadata(service, instanceInfo.getMetadataId());
        metadata.ifPresent(instanceMetadata -> InstanceUtil.updateInstanceMetadata(result, instanceMetadata));
        return result;
    }
}
