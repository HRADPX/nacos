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

import java.util.Collections;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.cluster.AbstractMemberLookup;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.sys.env.EnvUtil;

/**
 * Member node addressing mode in stand-alone mode.
 *
 * 单机寻址
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class StandaloneMemberLookup extends AbstractMemberLookup {
    
    @Override
    public void doStart() {
        // 根据 ip:port 组合信息，解析包装成 Member 对象，添加到 ServerMemberManager 中
        String url = EnvUtil.getLocalAddress();
        afterLookup(MemberUtil.readServerConf(Collections.singletonList(url)));
    }
    
    @Override
    protected void doDestroy() throws NacosException {
    
    }
    
    @Override
    public boolean useAddressServer() {
        return false;
    }
}
