package org.apache.eventmesh.dashboard.core.remoting;

import org.apache.eventmesh.dashboard.common.enums.ClusterTrusteeshipType;
import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.common.enums.RemotingType;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.ClusterRelationshipMetadata;
import org.apache.eventmesh.dashboard.common.model.metadata.RuntimeMetadata;
import org.apache.eventmesh.dashboard.common.model.remoting.GlobalRequest;
import org.apache.eventmesh.dashboard.common.model.remoting.GlobalResult;
import org.apache.eventmesh.dashboard.common.model.remoting.RemotingAction;
import org.apache.eventmesh.dashboard.core.cluster.ClusterDO;
import org.apache.eventmesh.dashboard.core.cluster.ColonyDO;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQAclRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQClientRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQConfigRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQGroupRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQOffsetRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQSubscriptionRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQTopicRemotingService;
import org.apache.eventmesh.dashboard.core.remoting.rocketmq.RocketMQUserRemotingService;
import org.apache.eventmesh.dashboard.service.remoting.IntegratedRemotingService;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Data;
import lombok.Getter;

/**
 * @author hahaha
 */
public class RemotingManager {


    private final Map<RemotingType, List<Class<?>>> remotingServiceClasses = new HashMap<>();

    /**
     * key clusterId
     */
    private final Map<Long, RemotingServiceContext> remotingServiceContextMap = new ConcurrentHashMap<>();

    @Getter
    private final Object methodProxy;

    /**
     * Long key  is clusterId
     */
    private final Map<Long, ColonyDO> colonyDOMap = new HashMap<>();

    private AtomicBoolean loading = new AtomicBoolean(true);


    {
        for (RemotingType remotingType : RemotingType.values()) {
            remotingServiceClasses.put(remotingType, new ArrayList<>());
        }

        // register implementation services
        this.registerService(RemotingType.ROCKETMQ, RocketMQAclRemotingService.class, RocketMQConfigRemotingService.class, RocketMQClientRemotingService.class
                , RocketMQGroupRemotingService.class, RocketMQOffsetRemotingService.class, RocketMQSubscriptionRemotingService.class
                , RocketMQTopicRemotingService.class, RocketMQUserRemotingService.class);
        this.registerService(RemotingType.EVENT_MESH_RUNTIME, RocketMQAclRemotingService.class, RocketMQConfigRemotingService.class, RocketMQClientRemotingService.class
                , RocketMQGroupRemotingService.class, RocketMQOffsetRemotingService.class, RocketMQSubscriptionRemotingService.class
                , RocketMQTopicRemotingService.class, RocketMQUserRemotingService.class);

        RemotingServiceHandler remotingServiceHandler = new RemotingServiceHandler();
        Class<?>[] clazzList = new Class[]{IntegratedRemotingService.class}; // only 1 element
        methodProxy = Proxy.newProxyInstance(this.getClass().getClassLoader(), clazzList, remotingServiceHandler);
    }

    public void registerService(RemotingType remotingType, Class<?>... clazzs) {
        List<Class<?>> serviceList = this.remotingServiceClasses.get(remotingType);
        Collections.addAll(serviceList, clazzs);
    }

    public void registerColony(ColonyDO colonyDO) throws Exception {
        if (loading.get() == true) {
            return;
        }

        if (colonyDO.getClusterDO().getClusterInfo().getClusterType().isEventMeshCluster()) {
            ClusterType clusterType = colonyDO.getClusterDO().getClusterInfo().getClusterType();
            RemotingType remotingType = clusterType.getRemotingType();
            List<Class<?>> remotingServersClassList = remotingServiceClasses.get(remotingType);
            Map<Class<?>, Object> remotingServiceCacheMap = new HashMap<>();
            for (Class<?> clazz : remotingServersClassList) {
                AbstractRemotingService abstractRemotingService = (AbstractRemotingService) clazz.newInstance();
                abstractRemotingService.setColonyDO(colonyDO);
                abstractRemotingService.init();
                remotingServiceCacheMap.put(clazz.getInterfaces()[0], abstractRemotingService);
            }
            RemotingServiceContext remotingServiceContext = new RemotingServiceContext();
            remotingServiceContext.setColonyDO(colonyDO);
            remotingServiceContext.setRemotingServiceImplMap(remotingServiceCacheMap);
            this.remotingServiceContextMap.put(colonyDO.getClusterId(), remotingServiceContext);
        } else {
            this.updateColony(colonyDO);
        }

    }

    public void updateColony(ColonyDO colonyDO) {
        RemotingServiceContext remotingServiceContext = this.getEventMeshRemotingServiceContext(colonyDO);
        /*
          There is a delay
         */
        if (Objects.isNull(remotingServiceContext)) {
            return;
        }
        ColonyDO mainColonyDO = this.getEventMeshColonyDO(colonyDO);
        for (Object object : remotingServiceContext.getRemotingServiceImplMap().values()) {
            AbstractRemotingService abstractRemotingService = (AbstractRemotingService) object;
            abstractRemotingService.setColonyDO(mainColonyDO);
            abstractRemotingService.update();
        }
    }

    public RemotingServiceContext getEventMeshRemotingServiceContext(ColonyDO colonyDO) {
        Long clusterId = colonyDO.getClusterDO().getClusterInfo().getClusterType().isEventMeshCluster() ? colonyDO.getClusterId() : colonyDO.getSuperiorId();
        if (Objects.isNull(clusterId)) {
            return null;
        }
        return remotingServiceContextMap.get(clusterId);
    }

    public ColonyDO getEventMeshColonyDO(ColonyDO colonyDO) {
        Long clusterId = colonyDO.getClusterDO().getClusterInfo().getClusterType().isEventMeshCluster() ? colonyDO.getClusterId() : colonyDO.getSuperiorId();
        return colonyDOMap.get(clusterId);
    }

    public void unregister(ColonyDO colonyDO) {
        remotingServiceContextMap.remove(colonyDO.getClusterId());
    }

    public void loadingCompleted() throws Exception {
        this.loading.set(false);
        for (ColonyDO colonyDO : colonyDOMap.values()) {
            if (colonyDO.getClusterDO().getClusterInfo().getClusterType().isEventMeshCluster()) {
                this.registerColony(colonyDO);
            }
        }
    }


    /**
     * 解除玩关系，才能删除
     *
     * @param clusterEntityList
     */
    public void cacheCluster(List<ClusterMetadata> clusterEntityList) {
        for (ClusterMetadata cluster : clusterEntityList) {
            Long clusterId = cluster.getId();
            if (cluster.getStatus() == 1) {
                ColonyDO colonyDO = colonyDOMap.remove(cluster.getClusterId());
                this.unregister(colonyDO);
                continue;
            }
            ColonyDO colonyDO = this.colonyDOMap.computeIfAbsent(clusterId, key -> {
                ColonyDO newColonyDO = new ColonyDO();
                ClusterDO newClusterDO = new ClusterDO();
                newColonyDO.setClusterDO(newClusterDO);
                return newColonyDO;
            });
            if (Objects.isNull(colonyDO.getClusterDO().getClusterInfo())) {
                colonyDO.getClusterDO().setClusterInfo(cluster);

                try {
                    this.registerColony(colonyDO);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            } else {
                colonyDO.getClusterDO().setClusterInfo(cluster);
                this.updateColony(colonyDO);
            }
        }
    }

    public void cacheRuntime(List<RuntimeMetadata> runtimeMeatadataList) {
        for (RuntimeMetadata runtimeMetadata : runtimeMeatadataList) {
            ColonyDO colonyDO = this.colonyDOMap.get(runtimeMetadata.getClusterId());
            if (Objects.equals(runtimeMetadata.getStatus(), 1)) {
                colonyDO.getClusterDO().getRuntimeMap().put(runtimeMetadata.getId(), runtimeMetadata);
            } else {
                colonyDO.getClusterDO().getRuntimeMap().remove(runtimeMetadata.getId());
            }
        }
    }

    /**
     * 解除关系是解除关系，不是删除
     *
     * @param clusterRelationshipEntityList
     */
    public void cacheClusterRelationship(List<ClusterRelationshipMetadata> clusterRelationshipEntityList) {
        for (ClusterRelationshipMetadata clusterRelationshipEntity : clusterRelationshipEntityList) {
            ClusterType relationshipType = clusterRelationshipEntity.getRelationshipType();
            ColonyDO colonyDO = this.colonyDOMap.get(clusterRelationshipEntity.getClusterId());
            if (Objects.equals(relationshipType.getAssemblyNodeType(), ClusterType.META)) {
                this.relationship(colonyDO, colonyDO.getMetaColonyDOList(), clusterRelationshipEntity);
            } else if (Objects.equals(relationshipType.getAssemblyNodeType(), ClusterType.RUNTIME)) {
                this.relationship(colonyDO, colonyDO.getRuntimeColonyDOList(), clusterRelationshipEntity);
            } else if (Objects.equals(relationshipType.getAssemblyNodeType(), ClusterType.STORAGE)) {
                this.relationship(colonyDO, colonyDO.getStorageBrokerColonyDOList(), clusterRelationshipEntity);
            }
        }
    }

    private void relationship(ColonyDO colonyDO, Map<Long, ColonyDO> clusterDOList, ClusterRelationshipMetadata clusterRelationshipMetadata) {
        if (Objects.equals(clusterRelationshipMetadata.getStatus(), 2)) {
            clusterDOList.remove(clusterRelationshipMetadata.getRelationshipId());
        } else {
            ColonyDO relationshiCcolonyDO = this.colonyDOMap.get(clusterRelationshipMetadata.getRelationshipId());
            clusterDOList.put(clusterRelationshipMetadata.getRelationshipId(), relationshiCcolonyDO);
            relationshiCcolonyDO.setSuperiorId(colonyDO.getClusterId());
        }
        this.updateColony(colonyDO);
    }

    public List<RemotingServiceContext> getEventMeshClusterDO(ClusterTrusteeshipType... clusterTrusteeshipType) {
        return this.filter(ClusterType.EVENTMESH, clusterTrusteeshipType);
    }

    public List<RemotingServiceContext> getMetaNacosClusterDO(ClusterTrusteeshipType... clusterTrusteeshipType) {
        return this.filter(ClusterType.EVENTMESH_META_ETCD, clusterTrusteeshipType);
    }

    public List<RemotingServiceContext> getMetaEtcdClusterDO(ClusterTrusteeshipType... clusterTrusteeshipType) {
        return this.filter(ClusterType.EVENTMESH_META_NACOS, clusterTrusteeshipType);
    }

    public List<RemotingServiceContext> getRocketMQClusterDO(ClusterTrusteeshipType... clusterTrusteeshipType) {
        return this.filter(ClusterType.STORAGE_ROCKETMQ, clusterTrusteeshipType);
    }

    public List<RemotingServiceContext> getStorageCluster(ClusterTrusteeshipType... clusterTrusteeshipType) {
        List<RemotingServiceContext> list = new ArrayList<>();
        for(ClusterType clusterType : ClusterType.STORAGE_TYPES){
            list.addAll(this.filter(clusterType, clusterTrusteeshipType));
        }
        return list;
    }

    public boolean isClusterTrusteeshipType(Long clusterId, ClusterTrusteeshipType clusterTrusteeshipType){
        ColonyDO colonyDO = this.colonyDOMap.get(clusterId  );
        if(Objects.isNull(colonyDO)){
            return false;
        }
        return Objects.equals(colonyDO.getClusterDO().getClusterInfo().getTrusteeshipType() , clusterTrusteeshipType);
    }

    /**
     * Return clusters with selected TrusteeType enabled.
     */
    private List<RemotingServiceContext> filter(ClusterType clusterType, ClusterTrusteeshipType... clusterTrusteeshipTypes) {
        Set<ClusterTrusteeshipType> clusterTrusteeshipType = new HashSet<>(Arrays.asList(clusterTrusteeshipTypes));
        List<RemotingServiceContext> remotingServiceContextList = new ArrayList<>();
        for (RemotingServiceContext remotingServiceContext : remotingServiceContextMap.values()) {
            ClusterMetadata clusterMetadata = remotingServiceContext.getColonyDO().getClusterDO().getClusterInfo();
            if (Objects.equals(clusterMetadata.getClusterType(), clusterType)) {
                if (clusterTrusteeshipType.contains(clusterMetadata.getTrusteeshipType())) {
                    remotingServiceContextList.add(remotingServiceContext);
                }
            }
        }
        return remotingServiceContextList;
    }


    public <T> T request(RemotingRequestWrapper remotingRequestWrapper, List<RemotingServiceContext> remotingServiceContextList) {
        List<Object> resultData = new ArrayList<>();

        Class<?> clazz = remotingRequestWrapper.getClass();
        Type superclass = clazz.getGenericSuperclass();
        Class<GlobalRequest> globalRequestClass = null;
        Class<?> executeClass = null;
        if (superclass instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superclass;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            executeClass = (Class<GlobalRequest>)actualTypeArguments[0];
            globalRequestClass = (Class<GlobalRequest>)actualTypeArguments[1];
        }
        RemotingRequestWrapper<Object,Object>  remotingRequestWrapper1 = (RemotingRequestWrapper<Object,Object>)remotingRequestWrapper;
        for (RemotingServiceContext remotingServiceContext : remotingServiceContextList) {
            try {
                GlobalRequest globalRequest = globalRequestClass.newInstance();
                globalRequest.setClusterId(remotingServiceContext.getColonyDO().getClusterId());
                GlobalResult<Object> globalResult = remotingRequestWrapper1.request(globalRequest,executeClass);
                if(globalResult.getData() instanceof  List){
                    resultData.addAll((List<Object>)globalResult.getData());
                }else{
                    resultData.add(globalResult.getData());
                }
            } catch (Exception e) {
                //TODO  There should be no abnormal occurrence of InstantiationException, IllegalAccessException, Exception
                //
            }
        }
        return (T) resultData;

    }

    public interface RemotingRequestWrapper<T,RE> {

        GlobalResult request(T t,RE key);
    }

    @Data
    public static class RemotingServiceContext {

        private ColonyDO colonyDO;

        private Map<Class<?>, Object> remotingServiceImplMap = new HashMap<>();
    }

    public class RemotingServiceHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            GlobalRequest globalRequest = (GlobalRequest) args[0];
            Long clusterId = globalRequest.getClusterId();
            // ClusterDO
            RemotingServiceContext remotingServiceContext = remotingServiceContextMap.get(clusterId);
            // 完整执行对象
            Class<?> declaringClass = method.getDeclaringClass();
            Object remotingServiceImpl = remotingServiceContext.getRemotingServiceImplMap().get(declaringClass);
            if (Objects.isNull(remotingServiceImpl)) {
                // TODO LOG
            }

            Method currentMethod = remotingServiceImpl.getClass().getMethod(method.getName(), method.getParameterTypes());
            RemotingAction annotations = currentMethod.getAnnotation(RemotingAction.class);
            if (Objects.nonNull(annotations)) {
                if (!annotations.support()) {
                    ColonyDO colonyDO = remotingServiceContext.getColonyDO();
                    Map<Long, ColonyDO> colonyDOMap1 = colonyDO.getStorageBrokerColonyDOList();
                    for (ColonyDO c : colonyDOMap1.values()) {
                        RemotingServiceContext newRemotingServiceContext = remotingServiceContextMap.get(c.getClusterId());
                        Object newObject = newRemotingServiceContext.getRemotingServiceImplMap().get(declaringClass);
                        return method.invoke(newObject, args);
                    }
                }
            }
            return method.invoke(remotingServiceImpl, args);
        }
    }
}
