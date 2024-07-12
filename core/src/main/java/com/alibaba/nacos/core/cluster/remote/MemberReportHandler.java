/*
 *
 *  * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.nacos.core.cluster.remote;

import org.springframework.stereotype.Component;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.cluster.remote.request.MemberReportRequest;
import com.alibaba.nacos.core.cluster.remote.response.MemberReportResponse;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.utils.Loggers;

/**
 * MemberReportHandler.
 *
 * @author : huangtianhui
 */
@Component
public class MemberReportHandler extends RequestHandler<MemberReportRequest, MemberReportResponse> {
    
    private final ServerMemberManager memberManager;
    
    public MemberReportHandler(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }
    
    @Override
    public MemberReportResponse handle(MemberReportRequest request, RequestMeta meta) throws NacosException {
        // 这个 node 是其他服务节点
        Member node = request.getNode();
        if (!node.check()) {
            MemberReportResponse result = new MemberReportResponse();
            result.setErrorInfo(400, "Node information is illegal");
            return result;
        }
        LoggerUtils.printIfDebugEnabled(Loggers.CLUSTER, "node state report, receive info : {}", node);
        // 收到其他节点的请求，表示这个节点是存活的，将节点状态设置为上线状态
        node.setState(NodeState.UP);
        // 重置失败次数
        node.setFailAccessCnt(0);
        // 当前节点更新其他节点
        memberManager.update(node);
        // 将自己返回返回
        return new MemberReportResponse(memberManager.getSelf());
    }
    
}
