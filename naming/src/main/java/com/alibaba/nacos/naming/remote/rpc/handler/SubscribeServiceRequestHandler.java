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

package com.alibaba.nacos.naming.remote.rpc.handler;

import org.springframework.stereotype.Component;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.remote.request.SubscribeServiceRequest;
import com.alibaba.nacos.api.naming.remote.response.SubscribeServiceResponse;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.trace.event.naming.SubscribeServiceTraceEvent;
import com.alibaba.nacos.common.trace.event.naming.UnsubscribeServiceTraceEvent;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.impl.EphemeralClientOperationServiceImpl;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;

/**
 * Handler to handle subscribe service.
 *
 * @author liuzunfei
 * @author xiweng.yy
 */
@Component
public class SubscribeServiceRequestHandler extends RequestHandler<SubscribeServiceRequest, SubscribeServiceResponse> {
    
    private final ServiceStorage serviceStorage;
    
    private final NamingMetadataManager metadataManager;
    
    private final EphemeralClientOperationServiceImpl clientOperationService;
    
    public SubscribeServiceRequestHandler(ServiceStorage serviceStorage, NamingMetadataManager metadataManager,
            EphemeralClientOperationServiceImpl clientOperationService) {
        this.serviceStorage = serviceStorage;
        this.metadataManager = metadataManager;
        this.clientOperationService = clientOperationService;
    }
    
    @Override
    @Secured(action = ActionTypes.READ)
    public SubscribeServiceResponse handle(SubscribeServiceRequest request, RequestMeta meta) throws NacosException {
        String namespaceId = request.getNamespace();
        String serviceName = request.getServiceName();
        String groupName = request.getGroupName();
        String app = request.getHeader("app", "unknown");
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        Service service = Service.newService(namespaceId, groupName, serviceName, true);
        // ip、port 封装订阅者
        Subscriber subscriber = new Subscriber(meta.getClientIp(), meta.getClientVersion(), app, meta.getClientIp(),
                namespaceId, groupedServiceName, 0, request.getClusters());
        // 获取当前服务的所有的实例
        // 具体实现逻辑：先从 ClientServiceIndexesManager#publisherIndexes 中获取当前服务注册的所有客户端id，
        // 再从 ClientManager 中根据客户端id 获取所有的客户端实例 InstancePublishInfo（保存了ip、port）等信息，
        // 组合封装后拿到当前服务的所有实例信息，缓存后返回给客户端（发起请求的客户端）
        ServiceInfo serviceInfo = ServiceUtil.selectInstancesWithHealthyProtection(serviceStorage.getData(service),
                metadataManager.getServiceMetadata(service).orElse(null), subscriber.getCluster(), false,
                true, subscriber.getIp());
        if (request.isSubscribe()) {
            // 客户端订阅当前服务
            clientOperationService.subscribeService(service, subscriber, meta.getConnectionId());
            NotifyCenter.publishEvent(new SubscribeServiceTraceEvent(System.currentTimeMillis(),
                    meta.getClientIp(), service.getNamespace(), service.getGroup(), service.getName()));
        } else {
            // 移除订阅
            clientOperationService.unsubscribeService(service, subscriber, meta.getConnectionId());
            NotifyCenter.publishEvent(new UnsubscribeServiceTraceEvent(System.currentTimeMillis(),
                    meta.getClientIp(), service.getNamespace(), service.getGroup(), service.getName()));
        }
        return new SubscribeServiceResponse(ResponseCode.SUCCESS.getCode(), "success", serviceInfo);
    }
}
