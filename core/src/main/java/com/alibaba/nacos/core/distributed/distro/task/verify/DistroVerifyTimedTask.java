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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import java.util.List;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroExecuteTaskExecuteEngine;
import com.alibaba.nacos.core.utils.Loggers;

/**
 * Timed to start distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyTimedTask implements Runnable {
    
    private final ServerMemberManager serverMemberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    private final DistroExecuteTaskExecuteEngine executeTaskExecuteEngine;
    
    public DistroVerifyTimedTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder,
            DistroExecuteTaskExecuteEngine executeTaskExecuteEngine) {
        this.serverMemberManager = serverMemberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.executeTaskExecuteEngine = executeTaskExecuteEngine;
    }
    
    @Override
    public void run() {
        try {
            List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("server list is: {}", targetServer);
            }
            // DistroClientDataProcessor.TYPE
            // targetServer: 除自身之外的所有其他节点，为了对比当前节点保存的其他节点版本是否和节点的版本一致
            for (String each : distroComponentHolder.getDataStorageTypes()) {
                verifyForDataStorage(each, targetServer);
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
        }
    }
    
    private void verifyForDataStorage(String type, List<Member> targetServer) {
        DistroDataStorage dataStorage = distroComponentHolder.findDataStorage(type);
        // 需要先等数据同步（startLoadTask）任务完成，这是通过定时线程池调度的
        if (!dataStorage.isFinishInitial()) {
            Loggers.DISTRO.warn("data storage {} has not finished initial step, do not send verify data",
                    dataStorage.getClass().getSimpleName());
            return;
        }
        // 当前节点负责的连接，同步数据的连接会被过滤..
        // 发送给集群除自身的其他节点
        List<DistroData> verifyData = dataStorage.getVerifyData();
        if (null == verifyData || verifyData.isEmpty()) {
            return;
        }
        // 除自身外的所有服务节点，遍历发送
        for (Member member : targetServer) {
            DistroTransportAgent agent = distroComponentHolder.findTransportAgent(type);
            if (null == agent) {
                continue;
            }
            executeTaskExecuteEngine.addTask(member.getAddress() + type,
                    new DistroVerifyExecuteTask(agent, verifyData, member.getAddress(), type));
        }
    }
}
