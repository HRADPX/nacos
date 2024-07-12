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

package com.alibaba.nacos.core.cluster.lookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.cluster.AbstractMemberLookup;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.file.FileChangeEvent;
import com.alibaba.nacos.sys.file.FileWatcher;
import com.alibaba.nacos.sys.file.WatchFileCenter;

/**
 * Cluster.conf file managed cluster member node addressing pattern.
 *
 * 文件寻址
 *   1）Macos 集群默认的寻址实现。文件寻址模式下，集群的每个节点都需要维护一个 cluster.conf 文件，
 * 文件中默认只需填写每个成员节点的 ip 信息，端口会自动选择默认端口 8848，如果有特殊需求修改端口
 * 信息，则需要将对应节点的端口信息补充完整。
 *   2）当 Nacos 节点启动时，会读取该文件内容，然后将文件内的 ip 解析为节点列表，调用 afterLookUp
 * 方法存到 ServerMemberManger 中
 *   3）如果发现集群扩缩容，需要修改每个 Nacos 节点下的 cluster.conf 文件，然后 Nacos 内部
 * 文件变动监听中心 {@link WatchFileCenter} 自动发现文件修改，重新读取文件内容、加载 ip 列表
 * 信息、更新新增节点。
 *    这种寻址模式有个比较大的缺点——运维成本比较大，同时也会有数据不一致的情况（修改某个文件失败），
 * @see #doStart()
 * @see AddressServerMemberLookup
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class FileConfigMemberLookup extends AbstractMemberLookup {
    
    private static final String DEFAULT_SEARCH_SEQ = "cluster.conf";
    
    private FileWatcher watcher = new FileWatcher() {
        @Override
        public void onChange(FileChangeEvent event) {
            readClusterConfFromDisk();
        }
        
        @Override
        public boolean interest(String context) {
            return StringUtils.contains(context, DEFAULT_SEARCH_SEQ);
        }
    };
    
    @Override
    public void doStart() throws NacosException {
        // 从配置文件中读取集群所有的服务节点
        readClusterConfFromDisk();
        
        // Use the inotify mechanism to monitor file changes and automatically
        // trigger the reading of cluster.conf
        try {
            // 添加一个监听器，监听配置文件的变更，当配置文件内容变更时，会执行 readClusterConfFromDisk 方法来添加/删除集群中的节点
            WatchFileCenter.registerWatcher(EnvUtil.getConfPath(), watcher);
        } catch (Throwable e) {
            Loggers.CLUSTER.error("An exception occurred in the launch file monitor : {}", e.getMessage());
        }
    }
    
    @Override
    public boolean useAddressServer() {
        return false;
    }
    
    @Override
    protected void doDestroy() throws NacosException {
        WatchFileCenter.deregisterWatcher(EnvUtil.getConfPath(), watcher);
    }
    
    private void readClusterConfFromDisk() {
        Collection<Member> tmpMembers = new ArrayList<>();
        try {
            List<String> tmp = EnvUtil.readClusterConf();
            tmpMembers = MemberUtil.readServerConf(tmp);
        } catch (Throwable e) {
            Loggers.CLUSTER
                    .error("nacos-XXXX [serverlist] failed to get serverlist from disk!, error : {}", e.getMessage());
        }
        
        afterLookup(tmpMembers);
    }
}
