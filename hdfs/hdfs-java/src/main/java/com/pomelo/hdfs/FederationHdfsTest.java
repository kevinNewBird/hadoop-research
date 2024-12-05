package com.pomelo.hdfs;

import static com.pomelo.hdfs.constant.HdfsConstant.FEDORATION_CLIENT;

/**
 * description: federation联邦机制集群的api测试
 * company: 北京海量数据有限公司
 * create by: zhaosong 2024/12/5
 * version: 1.0
 */
public class FederationHdfsTest extends BaseHdfsTest {

    @Override
    protected String url() {
        return FEDORATION_CLIENT;
    }
}
