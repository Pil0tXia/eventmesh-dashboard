package org.apache.eventmesh.dashboard.core.remoting;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterRelationshipMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.topic.CreateTopicRequest;
import org.apache.eventmesh.dashboard.service.remoting.IntegratedRemotingService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class RemotingManagerTest {

    private RemotingManager remotingManager = new RemotingManager();

    @Test
    public void init_test(){
        IntegratedRemotingService methodProxy = (IntegratedRemotingService) remotingManager.getMethodProxy();

        CreateTopicRequest createTopicRequest = new CreateTopicRequest();
        createTopicRequest.setClusterId(1L);

        methodProxy.createTopic(createTopicRequest);
    }

    @Test
    public void mock_overall_logic(){
        List<ClusterRelationshipMetadata> clusterRelationshipMetadataList = new ArrayList<>();
        AtomicLong clusterId = new AtomicLong(1);
        List<ClusterMetadata> clusterMetadataList = new ArrayList<>();

        //  两个 eventmesh 集群
        ClusterMetadata eventMeshCluster1 = new ClusterMetadata();
        eventMeshCluster1.setClusterType(ClusterType.EVENTMESH_CLUSTER);
        eventMeshCluster1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshCluster1);

        ClusterMetadata eventMeshCluster2 = new ClusterMetadata();
        eventMeshCluster2.setClusterType(ClusterType.EVENTMESH_CLUSTER);
        eventMeshCluster2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshCluster2);

        // 两个注册中心
        ClusterMetadata eventMeshMetaNacos1 = new ClusterMetadata();
        eventMeshMetaNacos1.setClusterType(ClusterType.EVENTMESH_META_NACOS);
        eventMeshMetaNacos1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshMetaNacos1);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster1,eventMeshMetaNacos1);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster2,eventMeshMetaNacos1);

        ClusterMetadata eventMeshMetaNacos2 = new ClusterMetadata();
        eventMeshMetaNacos2.setClusterType(ClusterType.EVENTMESH_META_NACOS);
        eventMeshMetaNacos2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshMetaNacos2);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster1,eventMeshMetaNacos2);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster2,eventMeshMetaNacos2);

        //  2个 eventmesh runtime
        ClusterMetadata eventMeshRuntime1 = new ClusterMetadata();
        eventMeshRuntime1.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        eventMeshRuntime1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshRuntime1);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster1,eventMeshRuntime1);

        ClusterMetadata eventMeshRuntime2 = new ClusterMetadata();
        eventMeshRuntime2.setClusterType(ClusterType.EVENTMESH_RUNTIME);
        eventMeshRuntime2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(eventMeshRuntime2);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster2,eventMeshRuntime2);

        // 两个 rocketmq 集群
        ClusterMetadata rocketMCluster1 = new ClusterMetadata();
        rocketMCluster1.setClusterType(ClusterType.STORAGE_ROCKETMQ_CLUSTER);
        rocketMCluster1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketMCluster1);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster1,rocketMCluster1);

        ClusterMetadata rocketMCluster2 = new ClusterMetadata();
        rocketMCluster2.setClusterType(ClusterType.STORAGE_ROCKETMQ_CLUSTER);
        rocketMCluster2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketMCluster2);
        this.relationship(clusterRelationshipMetadataList, eventMeshCluster2,rocketMCluster2);

        // 2个 rocketmq broker 集群
        ClusterMetadata rocketBroker1 = new ClusterMetadata();
        rocketBroker1.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER);
        rocketBroker1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketBroker1);
        this.relationship(clusterRelationshipMetadataList, rocketMCluster2,rocketBroker1);

        ClusterMetadata rocketBroker2 = new ClusterMetadata();
        rocketBroker2.setClusterType(ClusterType.STORAGE_ROCKETMQ_BROKER);
        rocketBroker2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketBroker2);
        this.relationship(clusterRelationshipMetadataList, rocketMCluster2,rocketBroker2);

        // 两个 nameserver
        ClusterMetadata rocketMQNameserver1 = new ClusterMetadata();
        rocketMQNameserver1.setClusterType(ClusterType.STORAGE_ROCKETMQ_NAMESERVER);
        rocketMQNameserver1.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketMQNameserver1);
        this.relationship(clusterRelationshipMetadataList, rocketBroker1,rocketMQNameserver1);
        this.relationship(clusterRelationshipMetadataList, rocketBroker2,rocketMQNameserver1);

        ClusterMetadata rocketMQNameserver2 = new ClusterMetadata();
        rocketMQNameserver2.setClusterType(ClusterType.STORAGE_ROCKETMQ_NAMESERVER);
        rocketMQNameserver2.setId(clusterId.incrementAndGet());
        clusterMetadataList.add(rocketMQNameserver2);
        this.relationship(clusterRelationshipMetadataList, rocketBroker1,rocketMQNameserver2);
        this.relationship(clusterRelationshipMetadataList, rocketBroker2,rocketMQNameserver2);

        try {
            remotingManager.cacheCluster(clusterMetadataList);
            remotingManager.cacheClusterRelationship(clusterRelationshipMetadataList);
            remotingManager.loadingCompleted();

            IntegratedRemotingService methodProxy = (IntegratedRemotingService)remotingManager.getMethodProxy();
            CreateTopicRequest createTopicRequest = new CreateTopicRequest();
            createTopicRequest.setClusterId(9L);
            methodProxy.createTopic(createTopicRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void relationship(List<ClusterRelationshipMetadata> clusterRelationshipMetadataList , ClusterMetadata clusterMetadata , ClusterMetadata relationship){
        clusterMetadata.setStatus(0);
        relationship.setStatus(0);

        ClusterRelationshipMetadata clusterRelationshipMetadata = new ClusterRelationshipMetadata();
        clusterRelationshipMetadata.setClusterType(clusterMetadata.getClusterType());
        clusterRelationshipMetadata.setClusterId(clusterMetadata.getId());
        clusterRelationshipMetadata.setRelationshipId(relationship.getId());
        clusterRelationshipMetadata.setRelationshipType(relationship.getClusterType());
        clusterRelationshipMetadataList.add(clusterRelationshipMetadata);
    }
}
