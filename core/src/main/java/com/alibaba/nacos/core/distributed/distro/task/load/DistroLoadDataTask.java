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

package com.alibaba.nacos.core.distributed.distro.task.load;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;

/**
 * Distro load data task.
 *
 * @author xiweng.yy
 */
public class DistroLoadDataTask implements Runnable {
    
    private final ServerMemberManager memberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    private final DistroConfig distroConfig;
    
    private final DistroCallback loadCallback;
    
    private final Map<String, Boolean> loadCompletedMap;
    
    public DistroLoadDataTask(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
            DistroConfig distroConfig, DistroCallback loadCallback) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroConfig = distroConfig;
        this.loadCallback = loadCallback;
        loadCompletedMap = new HashMap<>(1);
    }
    
    @Override
    public void run() {
        try {
            // 拉取&&同步逻辑
            load();
            // 如果没有完成，重新提交任务，可以看作是一个 while(true) 逻辑，直到同步成功任务才会结束
            if (!checkCompleted()) {
                GlobalExecutor.submitLoadDataTask(this, distroConfig.getLoadDataRetryDelayMillis());
            } else {
                loadCallback.onSuccess();
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot data success");
            }
        } catch (Exception e) {
            loadCallback.onFailed(e);
            Loggers.DISTRO.error("[DISTRO-INIT] load snapshot data failed. ", e);
        }
    }
    
    private void load() throws Exception {
        // 新节点启动，可能 ServerManager 还没有寻址完成，需要等一会会
        while (memberManager.allMembersWithoutSelf().isEmpty()) {
            Loggers.DISTRO.info("[DISTRO-INIT] waiting server list init...");
            TimeUnit.SECONDS.sleep(1);
        }
        // 获取数据对应的存储结构，如果为空表示 DistroClientComponentRegistry 这个 bean 还没有完成创建，也需要等一会
        while (distroComponentHolder.getDataStorageTypes().isEmpty()) {
            Loggers.DISTRO.info("[DISTRO-INIT] waiting distro data storage register...");
            TimeUnit.SECONDS.sleep(1);
        }
        // each: DistroClientDataProcessor.TYPE（Nacos:Naming:v2:ClientData）
        for (String each : distroComponentHolder.getDataStorageTypes()) {
            // 存储当前存储是否完成的数据结构
            if (!loadCompletedMap.containsKey(each) || !loadCompletedMap.get(each)) {
                // 实际的同步逻辑
                loadCompletedMap.put(each, loadAllDataSnapshotFromRemote(each));
            }
        }
    }
    
    private boolean loadAllDataSnapshotFromRemote(String resourceType) {
        // 用于发送请求，包装了 grpc 连接代理对象
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        // 用于执行数据同步逻辑
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == transportAgent || null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO-INIT] Can't find component for type {}, transportAgent: {}, dataProcessor: {}",
                    resourceType, transportAgent, dataProcessor);
            return false;
        }
        // 依次向集群里的每个节点拉数据，但是只要有一个成功即可退出
        for (Member each : memberManager.allMembersWithoutSelf()) {
            long startTime = System.currentTimeMillis();
            try {
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {}", resourceType, each.getAddress());
                // 封装拉数据请求（DistroDataRequest） -> DistroDataRequestHandler
                DistroData distroData = transportAgent.getDatumSnapshot(each.getAddress());
                Loggers.DISTRO.info("[DISTRO-INIT] it took {} ms to load snapshot {} from {} and snapshot size is {}.",
                        System.currentTimeMillis() - startTime, resourceType, each.getAddress(),
                        getDistroDataLength(distroData));
                // 处理拉到的数据，同步注册数据到当前节点，注意这里只是 ClientManager 里的信息，无注册表里的信息，
                // 注册表里的信息通过异步事件机制完成的，逻辑同客户端注册一致，这里设计的比较巧妙
                boolean result = dataProcessor.processSnapshot(distroData);
                Loggers.DISTRO
                        .info("[DISTRO-INIT] load snapshot {} from {} result: {}", resourceType, each.getAddress(),
                                result);
                if (result) {
                    // 设置状态为 true，退出
                    distroComponentHolder.findDataStorage(resourceType).finishInitial();
                    return true;
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("[DISTRO-INIT] load snapshot {} from {} failed.", resourceType, each.getAddress(), e);
            }
        }
        return false;
    }
    
    private static int getDistroDataLength(DistroData distroData) {
        return distroData != null && distroData.getContent() != null ? distroData.getContent().length : 0;
    }
    
    private boolean checkCompleted() {
        if (distroComponentHolder.getDataStorageTypes().size() != loadCompletedMap.size()) {
            return false;
        }
        for (Boolean each : loadCompletedMap.values()) {
            if (!each) {
                return false;
            }
        }
        return true;
    }
}
