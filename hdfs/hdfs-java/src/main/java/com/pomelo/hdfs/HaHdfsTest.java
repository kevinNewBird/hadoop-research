package com.pomelo.hdfs;

import static com.pomelo.hdfs.constant.HdfsConstant.HA_CLIENT;

/**
 * description: 伪分布式集群或者完全分布式集群的api测试
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/5
 * version: 1.0
 */
public class HaHdfsTest extends BaseHdfsTest {

    @Override
    protected String url() {
        return HA_CLIENT;
    }
}
