/*
 * Copyright 2017 HomeAdvisor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.homeadvisor.kafdrop.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.homeadvisor.kafdrop.config.CuratorConfiguration;
import com.homeadvisor.kafdrop.model.*;
import com.homeadvisor.kafdrop.util.BrokerChannel;
import com.homeadvisor.kafdrop.util.Version;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.api.GroupCoordinatorRequest;
import kafka.api.GroupCoordinatorResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import kafka.coordinator.GroupOverview;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;
import kafka.server.ConfigType;
import kafka.utils.ZKGroupDirs;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;


@Service
public class CuratorKafkaMonitor implements KafkaMonitor
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   @Autowired
   private CuratorFramework curatorFramework;

   @Autowired
   private ObjectMapper objectMapper;

   private PathChildrenCache brokerPathCache;
   private PathChildrenCache topicConfigPathCache;
   private TreeCache topicTreeCache;
   private TreeCache consumerTreeCache;
   private NodeCache controllerNodeCache;

   private Map<Integer, BrokerVO> brokerCache = new TreeMap<>();

   private AtomicInteger cacheInitCounter = new AtomicInteger();

   private ForkJoinPool threadPool;

   @Autowired
   private CuratorKafkaMonitorProperties properties;
   private Version kafkaVersion;

   private RetryTemplate retryTemplate;

   @PostConstruct
   public void start() throws Exception
   {
      try
      {
         kafkaVersion = new Version(properties.getKafkaVersion());
      }
      catch (Exception ex)
      {
         throw new IllegalStateException("Invalid kafka version: " + properties.getKafkaVersion(), ex);
      }

      threadPool = new ForkJoinPool(properties.getThreadPoolSize());

      FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
      backOffPolicy.setBackOffPeriod(properties.getRetry().getBackoffMillis());

      final SimpleRetryPolicy retryPolicy =
         new SimpleRetryPolicy(properties.getRetry().getMaxAttempts(),
                               ImmutableMap.of(InterruptedException.class, false,
                                               Exception.class, true));

      retryTemplate = new RetryTemplate();
      retryTemplate.setBackOffPolicy(backOffPolicy);
      retryTemplate.setRetryPolicy(retryPolicy);

      cacheInitCounter.set(4);

      brokerPathCache = new PathChildrenCache(curatorFramework, ZkUtils.BrokerIdsPath(), true);
      brokerPathCache.getListenable().addListener(new BrokerListener());
      brokerPathCache.getListenable().addListener((f, e) -> {
         if (e.getType() == PathChildrenCacheEvent.Type.INITIALIZED)
         {
            cacheInitCounter.decrementAndGet();
            LOG.info("Broker cache initialized");
         }
      });
      brokerPathCache.start(StartMode.POST_INITIALIZED_EVENT);

      //topicConfigPathCache = new PathChildrenCache(curatorFramework, ZkUtils.TopicConfigPath(), true);
      topicConfigPathCache = new PathChildrenCache(curatorFramework, ZkUtils.getEntityConfigRootPath(ConfigType.Topic()), true);
      topicConfigPathCache.getListenable().addListener((f, e) -> {
         if (e.getType() == PathChildrenCacheEvent.Type.INITIALIZED)
         {
            cacheInitCounter.decrementAndGet();
            LOG.info("Topic configuration cache initialized");
         }
      });
      topicConfigPathCache.start(StartMode.POST_INITIALIZED_EVENT);

      topicTreeCache = new TreeCache(curatorFramework, ZkUtils.BrokerTopicsPath());
      topicTreeCache.getListenable().addListener((client, event) -> {
         if (event.getType() == TreeCacheEvent.Type.INITIALIZED)
         {
            cacheInitCounter.decrementAndGet();
            LOG.info("Topic tree cache initialized");
         }
      });
      topicTreeCache.start();

      consumerTreeCache = new TreeCache(curatorFramework, ZkUtils.ConsumersPath());
      consumerTreeCache.getListenable().addListener((client, event) -> {
         if (event.getType() == TreeCacheEvent.Type.INITIALIZED)
         {
            cacheInitCounter.decrementAndGet();
            LOG.info("Consumer tree cache initialized");
         }
      });
      consumerTreeCache.start();

      controllerNodeCache = new NodeCache(curatorFramework, ZkUtils.ControllerPath());
      controllerNodeCache.getListenable().addListener(this::updateController);
      controllerNodeCache.start(true);
      updateController();
   }

   private String clientId()
   {
      return properties.getClientId();
   }

   private void updateController()
   {
      Optional.ofNullable(controllerNodeCache.getCurrentData())
         .map(data -> {
            try
            {
               Map controllerData = objectMapper.reader(Map.class).readValue(data.getData());
               return (Integer) controllerData.get("brokerid");
            }
            catch (IOException e)
            {
               LOG.error("Unable to read controller data", e);
               return null;
            }
         })
         .ifPresent(this::updateController);
   }

   private void updateController(int brokerId)
   {
      brokerCache.values()
         .forEach(broker -> broker.setController(broker.getId() == brokerId));
   }

   private void validateInitialized()
   {
      if (cacheInitCounter.get() > 0)
      {
         throw new NotInitializedException();
      }
   }


   @PreDestroy
   public void stop() throws IOException
   {
      consumerTreeCache.close();
      topicConfigPathCache.close();
      brokerPathCache.close();
      controllerNodeCache.close();
      close();
   }

   private int brokerId(ChildData input)
   {
      return Integer.parseInt(StringUtils.substringAfter(input.getPath(), ZkUtils.BrokerIdsPath() + "/"));
   }

   private BrokerVO addBroker(BrokerVO broker)
   {
      final BrokerVO oldBroker = brokerCache.put(broker.getId(), broker);
      LOG.info("Kafka broker {} was {}", broker.getId(), oldBroker == null ? "added" : "updated");
      return oldBroker;
   }

   private BrokerVO removeBroker(int brokerId)
   {
      final BrokerVO broker = brokerCache.remove(brokerId);
      LOG.info("Kafka broker {} was removed", broker.getId());
      return broker;
   }

   @Override
   public List<BrokerVO> getBrokers()
   {
      validateInitialized();
      return brokerCache.values().stream().collect(Collectors.toList());
   }

   @Override
   public Optional<BrokerVO> getBroker(int id)
   {
      validateInitialized();
      return Optional.ofNullable(brokerCache.get(id));
   }

   private BrokerChannel brokerChannel(Integer brokerId)
   {
      if (brokerId == null)
      {
         brokerId = randomBroker();
         if (brokerId == null)
         {
            throw new BrokerNotFoundException("No brokers available to select from");
         }
      }

      Integer finalBrokerId = brokerId;
      BrokerVO broker = getBroker(brokerId)
         .orElseThrow(() -> new BrokerNotFoundException("Broker " + finalBrokerId + " is not available"));

      return BrokerChannel.forBroker(broker.getHost(), broker.getPort());
   }

   private Integer randomBroker()
   {
      if (brokerCache.size() > 0)
      {
         List<Integer> brokerIds = brokerCache.keySet().stream().collect(Collectors.toList());
         Collections.shuffle(brokerIds);
         return brokerIds.get(0);
      }
      else
      {
         return null;
      }
   }

   @Override
   public List<TopicVO> getTopicsWithConsumers()
   {
      validateInitialized();
      List<TopicVO> topicList = getTopicMetadata().values().stream()
    	         .sorted(Comparator.comparing(TopicVO::getName))
    	         .collect(Collectors.toList());
      
      setConsumersForAllTopics(topicList);
      
      return topicList;
   }
   
   @Override
   public List<TopicVO> getTopics()
   {
	   validateInitialized();
	   List<TopicVO> topicList = getTopicMetadata().values().stream()
	    	         .sorted(Comparator.comparing(TopicVO::getName))
	    	         .collect(Collectors.toList());
	      
	   return topicList;   
   }

   @Override
   public Optional<TopicVO> getTopic(String topic)
   {
      validateInitialized();
      final Optional<TopicVO> topicVO = Optional.ofNullable(getTopicMetadata(topic).get(topic));
      topicVO.ifPresent(
         vo -> {
            getTopicPartitionSizes(vo, kafka.api.OffsetRequest.LatestTime())
               .entrySet()
               .forEach(entry -> vo.getPartition(entry.getKey()).ifPresent(p -> p.setSize(entry.getValue())));
            getTopicPartitionSizes(vo, kafka.api.OffsetRequest.EarliestTime())
               .entrySet()
               .forEach(entry -> vo.getPartition(entry.getKey()).ifPresent(p -> p.setFirstOffset(entry.getValue())));
         }
      );
      return topicVO;
   }

   private Map<String, TopicVO> getTopicMetadata(String... topics)
   {
      if (kafkaVersion.compareTo(new Version(0, 9, 0)) >= 0)
      {
         return retryTemplate.execute(
            context -> brokerChannel(null)
               .execute(channel -> getTopicMetadata(channel, topics)));
      }
      else
      {
         Stream<String> topicStream;
         if (topics == null || topics.length == 0)
         {
            topicStream =
               Optional.ofNullable(
                  topicTreeCache.getCurrentChildren(ZkUtils.BrokerTopicsPath()))
                  .map(Map::keySet)
                  .map(Collection::stream)
                  .orElse(Stream.empty());
         }
         else
         {
            topicStream = Stream.of(topics);
         }

         return topicStream
            .map(this::getTopicZkData)
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(TopicVO::getName, topic -> topic));
      }
   }

   private TopicVO getTopicZkData(String topic)
   {
      return Optional.ofNullable(topicTreeCache.getCurrentData(ZkUtils.getTopicPath(topic)))
         .map(this::parseZkTopic)
         .orElse(null);
   }

   public TopicVO parseZkTopic(ChildData input)
   {
      try
      {
         final TopicVO topic = new TopicVO(StringUtils.substringAfterLast(input.getPath(), "/"));

         final TopicRegistrationVO topicRegistration =
            objectMapper.reader(TopicRegistrationVO.class).readValue(input.getData());

         topic.setConfig(
           /* Optional.ofNullable(topicConfigPathCache.getCurrentData(ZkUtils.TopicConfigPath() + "/" + topic.getName()))*/
        		 Optional.ofNullable(topicConfigPathCache.getCurrentData(ZkUtils.getEntityConfigRootPath(ConfigType.Topic()) + "/" + topic.getName()))
        		 .map(this::readTopicConfig)
               .orElse(Collections.emptyMap()));

         for (Map.Entry<Integer, List<Integer>> entry : topicRegistration.getReplicas().entrySet())
         {
            final int partitionId = entry.getKey();
            final List<Integer> partitionBrokerIds = entry.getValue();

            final TopicPartitionVO partition = new TopicPartitionVO(partitionId);

            final TopicPartitionStateVO partitionState = partitionState(topic.getName(), partition.getId());

            partitionBrokerIds.stream()
               .map(brokerId -> {
                  TopicPartitionVO.PartitionReplica replica = new TopicPartitionVO.PartitionReplica();
                  replica.setId(brokerId);
                  replica.setInService(partitionState.getIsr().contains(brokerId));
                  replica.setLeader(brokerId == partitionState.getLeader());
                  return replica;
               })
               .forEach(partition::addReplica);

            topic.addPartition(partition);
         }

         // todo: get partition sizes here as single bulk request?

         return topic;
      }
      catch (IOException e)
      {
         throw Throwables.propagate(e);
      }
   }

   private Map<String, TopicVO> getTopicMetadata(BlockingChannel channel, String... topics)
   {
      final TopicMetadataRequest request =
         new TopicMetadataRequest((short) 0, 0, clientId(), Arrays.asList(topics));

      LOG.debug("Sending topic metadata request: {}", request);

      channel.send(request);
      final kafka.api.TopicMetadataResponse underlyingResponse =
    		  kafka.api.TopicMetadataResponse.readFrom(channel.receive().payload());
        // kafka.api.TopicMetadataResponse.readFrom(channel.receive().buffer());

      LOG.debug("Received topic metadata response: {}", underlyingResponse);

      TopicMetadataResponse response = new TopicMetadataResponse(underlyingResponse);
      return response.topicsMetadata().stream()
         .filter(tmd -> tmd.errorCode() == ErrorMapping.NoError())
         .map(this::processTopicMetadata)
         .collect(Collectors.toMap(TopicVO::getName, t -> t));
   }

   private TopicVO processTopicMetadata(TopicMetadata tmd)
   {
      TopicVO topic = new TopicVO(tmd.topic());

      topic.setConfig(
//         Optional.ofNullable(topicConfigPathCache.getCurrentData(ZkUtils.TopicConfigPath() + "/" + topic.getName()))
          Optional.ofNullable(topicConfigPathCache.getCurrentData(ZkUtils.getEntityConfigRootPath(ConfigType.Topic()) + "/" + topic.getName()))
            .map(this::readTopicConfig)
            .orElse(Collections.emptyMap()));

      topic.setPartitions(
         tmd.partitionsMetadata().stream()
            .map((pmd) -> parsePartitionMetadata(tmd.topic(), pmd))
            .collect(Collectors.toMap(TopicPartitionVO::getId, p -> p))
      );
      return topic;
   }

   private TopicPartitionVO parsePartitionMetadata(String topic, PartitionMetadata pmd)
   {
      TopicPartitionVO partition = new TopicPartitionVO(pmd.partitionId());
      if (pmd.leader() != null)
      {
         partition.addReplica(new TopicPartitionVO.PartitionReplica(pmd.leader().id(), true, true));
      }

      final List<Integer> isr = getIsr(topic, pmd);
      pmd.replicas().stream()
         .map(replica -> new TopicPartitionVO.PartitionReplica(replica.id(), isr.contains(replica.id()), false))
         .forEach(partition::addReplica);
      return partition;
   }

   private List<Integer> getIsr(String topic, PartitionMetadata pmd)
   {
      //return pmd.isr().stream().map(Broker::id).collect(Collectors.toList());
	   return pmd.isr().stream().map(BrokerEndPoint::id).collect(Collectors.toList());
   }

   private Map<String, Object> readTopicConfig(ChildData d)
   {
      try
      {
         final Map<String, Object> configData = objectMapper.reader(Map.class).readValue(d.getData());
         return (Map<String, Object>) configData.get("config");
      }
      catch (IOException e)
      {
         throw Throwables.propagate(e);
      }
   }


   private TopicPartitionStateVO partitionState(String topicName, int partitionId)
      throws IOException
   {
      return objectMapper.reader(TopicPartitionStateVO.class).readValue(
         topicTreeCache.getCurrentData(
            ZkUtils.getTopicPartitionLeaderAndIsrPath(topicName, partitionId))
            .getData());
   }

   
   private class ConsumerGroupVersion10{
	   private String topic;
	   private List<String> topics = new ArrayList<String>();
	   private int partition;
	   private int currentOffest;
	   private int logEndOffset;
	   private int lag;
	   private String owner;
	   

	   public ConsumerGroupVersion10(List<String> topics){
		   super();
			this.topics = topics;
		}
	   
	   public ConsumerGroupVersion10(String topic, int partition, int currentOffest, int logEndOffset, int lag, String owner) {
		super();
		this.topic = topic;
		this.partition = partition;
		this.currentOffest = currentOffest;
		this.logEndOffset = logEndOffset;
		this.lag = lag;
		this.owner = owner;
	}

	   public ConsumerGroupVersion10(String[] allMembers) {
		   this.topic = allMembers[1];
		   this.partition = returnIntegerValue(allMembers[2]);
		   this.currentOffest = returnIntegerValue(allMembers[3]);
		   this.logEndOffset = returnIntegerValue(allMembers[4]);
		   this.lag = returnIntegerValue(allMembers[5]);
		   this.owner = allMembers[6];
	   }

	   
		
	private int returnIntegerValue(String string) {
       try {
           return Integer.valueOf(string).intValue();
       } catch (NumberFormatException e) {
           return -1;
       }
   }   
	
	
	public String getTopic() {
		return topic;
	}


	public int getPartition() {
		return partition;
	}


	public int getCurrentOffest() {
		return currentOffest;
	}


	public int getLogEndOffset() {
		return logEndOffset;
	}


	public int getLag() {
		return lag;
	}


	public String getOwner() {
		return owner;
	}

	public boolean isMatchTopics(String topic){
		return topic.equals(this.getTopic());
	}
	
	   public void addTopicToTheList (String topic){
		   this.topics.add(topic);
	   }
	   
	   public boolean isContainsTopic(String topic){
		   return topics.contains(topic);
	   }
   }
   

   private List<String> execCommand(String commandString)
   {
	   Process prc;
	   	   	   
	   try {
			prc = Runtime.getRuntime().exec(commandString);
			BufferedReader input = new BufferedReader(new InputStreamReader(prc.getInputStream()));
	
			return input.lines().collect(Collectors.toList());
	   } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	   }
	   
	   
	   return null; 
   }
   
  
   @Autowired
   private CuratorConfiguration.ZookeeperProperties zookeeperProperties;
   
   private String bootstrap = null;
   
   private void initializeBootstrap(){
	   
	   String connect = zookeeperProperties.getConnect();
	   BrokerVO currentBroker = this.getBrokers().get(0);
	   String host = connect.split(":")[0];
	   String port = String.valueOf(currentBroker.getPort());
	   if(host!=null && port!=null){
		   bootstrap = host+":"+port;
	   }else{
		   bootstrap = System.getProperty("bootstrap");
	   }
   }
   
   @Override
   public String getBootstrap(){
	   if(bootstrap == null){
		   initializeBootstrap();
	   }
	   return bootstrap;
   }
   
   private AdminClient adminClient= null;
   
   private AdminClient getAdminClient(){
	   if(adminClient == null){
		   adminClient = createAdminClient();
	   }
	   return adminClient;
   }
   
   private List<String> getConsumersString()
   {
	   
	   scala.collection.immutable.List<GroupOverview> test= getAdminClient().listAllConsumerGroupsFlattened();
	   List<String> listOfConsumersGroupsIDs = new ArrayList<String>();
	   
	   scala.collection.Iterator iter = test.iterator();
	   GroupOverview item;
	   while (iter.hasNext()) {
		   item = (GroupOverview) iter.next();
		   listOfConsumersGroupsIDs.add(item.groupId());
	   }

	   return listOfConsumersGroupsIDs;
/*	   String command = System.getProperty("command");
	   String listConsumerCommand = new String(command+ " --new-consumer --bootstrap-server "+ getBootstrap()+ " --list");
	   	   
	   List<String> list = execCommandOrig(listConsumerCommand);
	   //String[] args = {"--new-consumer","--bootstrap-server="+ getBootstrap(), "--list"};
	   //List<String> list = execCommand(args);
	      
	   return list;*/
   }
   
   
   private List<ConsumerSummary> getConsumersDescriptionByGroupId(String groupId)
   {
	   
	   scala.collection.immutable.List<ConsumerSummary> test= getAdminClient().describeConsumerGroup(groupId);
	   List<ConsumerSummary> listOfConsumersSummary = new ArrayList<ConsumerSummary>();
	   
	   scala.collection.Iterator iter = test.iterator();
	   ConsumerSummary item;
	   while (iter.hasNext()) {
		   item = (ConsumerSummary) iter.next();
		   listOfConsumersSummary.add(item);
	   }

	   return listOfConsumersSummary;

   }
   
   @Override
   public List<ConsumerVO> getConsumers()
   {
	   return null;
   }
   
   private AdminClient createAdminClient(){
	   Properties props = new Properties();
	   props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "--bootstrap-server="+getBootstrap());
	   return AdminClient.create(props);
   }
   
   private Map<String, List<ConsumerGroupVersion10>> getSpecificConsumerGroupDescription(String groupId)
   {
	   Map<String, List<ConsumerGroupVersion10>> result = new HashMap<String, List<ConsumerGroupVersion10>>();
	   
	   List<ConsumerSummary> commandList = getConsumersDescriptionByGroupId(groupId);
	   
	   String owner;
	   String topicName;
	   int partition;
	   OffsetAndMetadata partitionOffset;
	   int logEndOffset;
	   int currentOffset;
	   int lag;
	   ConsumerGroupVersion10 currConsumerGroup;
	   List<ConsumerGroupVersion10> consumerGrouplist;
	   for(ConsumerSummary consumerSummary: commandList)
	   {
		   owner = consumerSummary.clientId()+"_"+consumerSummary.clientHost();
		   
		   TopicPartition topicPartition;
		   scala.collection.Iterator<TopicPartition> iterator = consumerSummary.assignment().iterator();
		   while(iterator.hasNext())
		   {
			   topicPartition = iterator.next();
			   

			   topicName = topicPartition.topic();
			   partition = topicPartition.partition();
			   partitionOffset = getKafkaConsumer(groupId).committed(new TopicPartition(topicName,partition));
			   logEndOffset = (int)getLogEndOffset(topicName, partition,groupId);
			   if(partitionOffset != null){
				   currentOffset = (int)partitionOffset.offset();
					   lag = logEndOffset - currentOffset;
			   }else{
				   currentOffset = -1;
				   lag = -1;
			   }
			   
			   currConsumerGroup = buildAllConsumerGroupData(topicName,
					   partition,
					   currentOffset,
					   logEndOffset,
					   lag,
					   owner);
			   
			   if (result.containsKey(groupId))
			   {
				   consumerGrouplist = result.get(groupId);
				   consumerGrouplist.add(currConsumerGroup);
			   }
			   else
			   {
				   consumerGrouplist = new ArrayList<CuratorKafkaMonitor.ConsumerGroupVersion10>();
				   consumerGrouplist.add(currConsumerGroup);
				   result.put(groupId, consumerGrouplist);
			   }
		   
		   }
		   kafkaConsumer = null;
	   }
	   
	   return result;
   }
    
   private Map<String,ConsumerGroupVersion10> getAllOrSpecificConsumerGroupDescription(String groupId)
   {


	// String bootstrap = System.getProperty("bootstrap");
	   String command = System.getProperty("command");
	   
	   String groupDetailsCommand = new String(command+ " --new-consumer --bootstrap-server "+ getBootstrap()+ " --describe --group ");
	   
	   List<String> topicsPerGroup = null;
	   ConsumerSummary list2;
	   String list;
	   Map<String,ConsumerGroupVersion10> result = new HashMap<String,ConsumerGroupVersion10>();
	   
	   //only for presenting the consumer-groups 
	   if (groupId == null){
		   
		   final List<String>allGroupIdsList = getConsumersString();
		   //allGroupIdsList.forEach(item->execCommand(groupDetailsCommand+item));
		   for(int i=0; i<allGroupIdsList.size(); i++){
			   String prepForExec = allGroupIdsList.get(i);
			   List<ConsumerSummary> commandList = getConsumersDescriptionByGroupId(prepForExec);
			   //List<String> commandList = getConsumersDescriptionByGroupId(prepForExec);
			   //String[] args = {"--new-consumer", "--bootstrap-server="+ getBootstrap(), "--describe","--group="+prepForExec};
			  // List<String> commandList = execCommand(groupDetailsCommand+prepForExec);
			   //List<String> commandList = execCommand(args);
			   //list = commandList.get(2);
/*			   if(commandList.size()>1){
				   list = commandList.get(1);
				   result.put(allGroupIdsList.get(i), new ConsumerGroupVersion10(list.trim().split("\\s+")));
			   }
*/			   topicsPerGroup = new ArrayList<>();
			   for(int k=0; k<commandList.size(); k++){
				   list2 = commandList.get(k);
				   if(list2 != null){
					   scala.collection.immutable.List<TopicPartition> topicPartitionList = list2.assignment();
					   if(topicPartitionList!=null && topicPartitionList.size() > 0){
						   topicsPerGroup.add(topicPartitionList.head().topic());
					   }
				   }
			   }
			   result.put(allGroupIdsList.get(i), new ConsumerGroupVersion10(topicsPerGroup));
				   
			   
		   }
	   }
	 //need to present all consumers under a consumer-group 
	   else{
		   if(command != null){
			   List<String> list1 = execCommand(groupDetailsCommand+groupId);
			   int j = 1;
				while (j < list1.size()) {
					list = list1.get(j);
					result.put(groupId+"_"+String.valueOf(j-1), new ConsumerGroupVersion10(list.trim().split("\\s+")));
					j++;
				}
		   }
/*	   

------------------------------------------------------------------------------------------------------
 protected def describeGroup(group: String) {
      val consumerSummaries = adminClient.describeConsumerGroup(group)
      if (consumerSummaries.isEmpty)
        println(s"Consumer group `${group}` does not exist or is rebalancing.")
      else {
        val consumer = getConsumer()
        printDescribeHeader()
        consumerSummaries.foreach { consumerSummary =>
          val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
          val partitionOffsets = topicPartitions.flatMap { topicPartition =>
            Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata =>
              topicPartition -> offsetAndMetadata.offset
            }
          }.toMap
          describeTopicPartition(group, topicPartitions, partitionOffsets.get,
            _ => Some(s"${consumerSummary.clientId}_${consumerSummary.clientHost}"))
        }
      }
    }		   
 */
		   else{
			   List<ConsumerSummary> commandList = getConsumersDescriptionByGroupId(groupId);
			   List<ConsumerSummary> listOfConsumersSummary = new ArrayList<ConsumerSummary>();
	   
			   
			  // KafkaConsumer<String, String> consumer = getKafkaConsumer(groupId);

			  Iterator<ConsumerSummary> iter = commandList.iterator();
			   ConsumerSummary item;
			   TopicVO topic;
			   int k=0;
			   OffsetAndMetadata partitionOffset;
//			   TopicVO topic1 = new TopicVO(commandList.get(0).assignment().head().topic());
//			   Optional<TopicVO> topic2  = this.getTopic(topic1.getName());
			   String topicName;int partition;int currentOffset; int logEndOffset;int lag; String owner; TopicPartition topicPartition;
			 //TODO::: need to check for all the topicPartiions if working as can be a list in a ConsumerSummary
			   while (iter.hasNext()) {
				   item = (ConsumerSummary) iter.next();
				   owner = item.clientId()+"_"+item.clientHost();
				   scala.collection.Iterator<TopicPartition> iter1 = item.assignment().iterator();
				   while (iter1.hasNext()){
					   topicPartition= iter1.next();
					   topicName = topicPartition.topic();
					   partition = topicPartition.partition();
					   partitionOffset = getKafkaConsumer(groupId).committed(new TopicPartition(topicName,partition));
					   logEndOffset = (int)getLogEndOffset(topicName, partition,groupId);
					   if(partitionOffset != null){
						   currentOffset = (int)partitionOffset.offset();
	  					   lag = logEndOffset - currentOffset;
					   }else{
						   currentOffset = -1;
						   lag = -1;
					   }
					   
				   //topic = new TopicVO(item.assignment().head().topic());
				   //topic.addPartition(new TopicPartitionVO(item.assignment().head().partition()));
				   result.put(groupId+"_"+String.valueOf(k), buildAllConsumerGroupData(topicName,
						   partition,
						   currentOffset,
						   logEndOffset,
						   lag,
						   owner));
				   k++;
				   }
				   
			   }
		   }
	   }
	   kafkaConsumer = null;
	   return result;
   }
   
   private ConsumerGroupVersion10 buildAllConsumerGroupData(String topicName,int partition,int currentOffset, int logEndOffset, int lag, String owner){
	   
	   return new ConsumerGroupVersion10(topicName, partition, currentOffset, logEndOffset, lag, owner);
   }
   
   //-----------------------------------------------------------------------------------------------------
  // get kafka Consumer to get partitionsOffset
   
   private KafkaConsumer<String,String> kafkaConsumer = null;
   
   
   private KafkaConsumer<String,String> getKafkaConsumer(String groupId){
      if (kafkaConsumer == null){
    	  kafkaConsumer = createNewConsumer(groupId);  
      }
        
      return kafkaConsumer;
    }

    private KafkaConsumer<String, String> createNewConsumer(String groupId){
      Properties properties = new Properties();
      String deserializer = (new StringDeserializer()).getClass().getName();
      String brokerUrl = bootstrap;
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
      //if (opts.options.has(opts.commandConfigOpt)) properties.putAll(Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)))

      return new KafkaConsumer(properties);
    }
   
    private void close() {
        adminClient.close();
        if (kafkaConsumer != null) kafkaConsumer.close();
      }
    private long getLogEndOffset(String topic, int partition, String groupId){
    			KafkaConsumer<String, String> consumer = getKafkaConsumer(groupId);
    	      TopicPartition topicPartition = new TopicPartition(topic, partition);
    	      List<TopicPartition> listTopicPartition = new ArrayList<>();
    	      listTopicPartition.add(topicPartition);
    	      consumer.assign(listTopicPartition);
    	      consumer.seekToEnd(listTopicPartition);
    	      long logEndOffset = consumer.position(topicPartition);
    	      //LogEndOffsetResult. LogEndOffset(logEndOffset);
    	      return logEndOffset;
   }
   
   //---------------------------------------------------------------------------------------------------
  
   private Map<String, TopicVO> createMapFromTopicList(List<TopicVO> topics)
   {
	   Map<String, TopicVO> result = new HashMap<String, TopicVO>();
	   
	   for (TopicVO topic : topics)
	   {
		   result.put(topic.getName(), topic);
	   }
	   return result;
   }
   
   private Map<String, List<ConsumerGroupVersion10>> collectAllConsumers()
   {
	   Map<String, List<ConsumerGroupVersion10>> result = new HashMap<String, List<ConsumerGroupVersion10>>();
	   
	   final List<String> allGroupIdsList = getConsumersString();
	   
	   for (String groupId : allGroupIdsList)
	   {
		   result.putAll(getSpecificConsumerGroupDescription(groupId));
		   
	   }
	   return result;
   }
   public void setConsumersForAllTopics(List<TopicVO> topics)
   {
	   Map<String, TopicVO> topicsMap = createMapFromTopicList(topics);
	   
	   Map<String, List<ConsumerGroupVersion10>> consumerMap = collectAllConsumers();
	   
	   List<ConsumerGroupVersion10> currConsumerList = null;
	   String topicName;
	   TopicVO currTopic;
	   Set<ConsumerVO> currConsumerVOList;
	   ConsumerVO consumerVO;
	   for (Map.Entry<String, List<ConsumerGroupVersion10>> entry : consumerMap.entrySet())
	   {
		   currConsumerList = entry.getValue();
		   for (ConsumerGroupVersion10 consumerGroupDetails : currConsumerList)
		   {
			   topicName = consumerGroupDetails.getTopic();
			   currTopic = topicsMap.get(topicName);
			   consumerVO = map(entry.getKey(), consumerGroupDetails);
			   if (isLagExist(topicName, consumerVO))
			   {
				   if (currTopic.getConsumers() == null)
				   {
					   currConsumerVOList = new HashSet<ConsumerVO>();
					   currConsumerVOList.add(consumerVO);
					   currTopic.setConsumers(currConsumerVOList);
				   }
				   else
				   {
					   currConsumerVOList = currTopic.getConsumers();
					   currConsumerVOList.add(consumerVO);
				   }
			   }
		   }
	   }
   }
   
   private boolean isLagExist(String topicName, ConsumerVO consumerVO)
   {
	   ConsumerTopicVO consumerTopicVO = consumerVO.getTopic(topicName);
	   if (consumerTopicVO.getLag() != 0)
	   {
		   return true;
	   }
	   return false;
   }
   private ConsumerVO map(String groupId, ConsumerGroupVersion10 consumerGroupVersion10)
   {
	   ConsumerVO consumerVO = new ConsumerVO(groupId);
	   ConsumerPartitionVO consumerPartition = new ConsumerPartitionVO(groupId, consumerGroupVersion10.getTopic(), consumerGroupVersion10.getPartition());
	   consumerPartition.setOffset(consumerGroupVersion10.getCurrentOffest());
	   consumerPartition.setSize(consumerGroupVersion10.getLogEndOffset());
	   consumerPartition.setOwner(consumerGroupVersion10.getOwner());
	   ConsumerTopicVO consumerTopic = new ConsumerTopicVO(consumerGroupVersion10.getTopic());
	   Optional<TopicVO> topicForFirstOffset = this.getTopic(consumerTopic.getTopic());
	   consumerPartition.setFirstOffset(topicForFirstOffset.get().getPartition(consumerGroupVersion10.getPartition()).get().getFirstOffset());
	   consumerTopic.addOffset(consumerPartition);
	   consumerVO.addTopic(consumerTopic);
	   
	   return consumerVO;
   }
   @Override
   public List<ConsumerVO> getConsumers(final TopicVO topic)
   {
	   List<ConsumerVO> result = new ArrayList<>();
	   Map<String,ConsumerGroupVersion10> groupDetailsPerRelevantTopic = new HashMap<String,ConsumerGroupVersion10>();
	   
	   groupDetailsPerRelevantTopic = getAllOrSpecificConsumerGroupDescription(null);
   
	   for (Map.Entry<String,ConsumerGroupVersion10> entry : groupDetailsPerRelevantTopic.entrySet())
	   {
			if(entry.getValue().isContainsTopic(topic.getName())){
				result.add(new ConsumerVO(entry.getKey()));
			}
			
		}
	
	   result.forEach(consumer->consumer.addTopic(new ConsumerTopicVO(topic.getName())));
	   return result;
   }
   
   
   
   
   @Override
   public List<ConsumerVO> getConsumers(final String topic)
   {
      return getConsumers(getTopic(topic).get());
   }

   /*private Stream<ConsumerVO> getConsumerStream(TopicVO topic)
   {
	   return consumerTreeCache.getCurrentChildren(ZkUtils.ConsumersPath()).keySet().stream()
	     .map(g -> getConsumerByTopic(g, topic))
         .filter(Optional::isPresent)
         .map(Optional::get)
         .sorted(Comparator.comparing(ConsumerVO::getGroupId));
   }
*/
   private Stream<ConsumerVO> getConsumerStream(TopicVO topic)
   {
	   return getConsumersString().stream()
	     .map(g -> getConsumerByTopic(g, topic))
         .filter(Optional::isPresent)
         .map(Optional::get)
         .sorted(Comparator.comparing(ConsumerVO::getGroupId));
   }

/* 
 * Original  
 * 
 * @Override
   public Optional<ConsumerVO> getConsumer(String groupId)
   {
      validateInitialized();
      return getConsumerByTopic(groupId, null);
   }
*/
   
   @Override
   public Optional<ConsumerVO> getConsumer(String groupId)
   {
	  /* try
	   {
		   jmxManagement();
	   }
	   catch(Exception e){
		   e.printStackTrace();
	   }*/
	   
	   Optional<TopicVO> topicForFirstOffset = null;
	   ConsumerVO consumer = null;
	   ConsumerTopicVO consumerTopic = null;
	   ConsumerRegistrationVO consumerRegistration = null;
	   ConsumerPartitionVO consumerPartition = null;
/*	   ConsumerGroup consumerGroupDetails = null;
	   Map<String,ConsumerGroup> groupDetailsPerRelevantTopic = new HashMap<String,ConsumerGroup>();
	*/   
	   
	   ConsumerGroupVersion10 consumerGroupDetails = null;
	   Map<String,ConsumerGroupVersion10> groupDetailsPerRelevantTopic = new HashMap<String,ConsumerGroupVersion10>();
	
	   
	   consumer = new ConsumerVO(groupId);
	   groupDetailsPerRelevantTopic = getAllOrSpecificConsumerGroupDescription(groupId);
	   
	   //for(int i=1; i<=groupDetailsPerRelevantTopic.size(); i++){
	   for(int i=0; i<groupDetailsPerRelevantTopic.size(); i++){
	   consumerGroupDetails = groupDetailsPerRelevantTopic.get(groupId+"_"+i);

	   
	   //ConsumerPartition section
	   
	   consumerPartition = new ConsumerPartitionVO(groupId, consumerGroupDetails.getTopic(), consumerGroupDetails.getPartition());
	   consumerPartition.setOffset(consumerGroupDetails.getCurrentOffest());
	   consumerPartition.setSize(consumerGroupDetails.getLogEndOffset());
	   //consumerPartition.setOwner(consumerGroupDetails.getConsumerId());
	   consumerPartition.setOwner(consumerGroupDetails.getOwner());
	   
	   //TODO::::
	   //ConsumerRegistrationVO section
	   
	   
	   //ConsumerTopic section
	   consumerTopic = consumer.getTopic(consumerGroupDetails.getTopic());
	   if(consumerTopic==null){
		   consumerTopic = new ConsumerTopicVO(consumerGroupDetails.getTopic());
	   }
	   ///TODO::: need to represent the first offset and check how is it 0.
	   topicForFirstOffset = this.getTopic(consumerTopic.getTopic());
//	   topicForFirstOffset.get().getPartition(consumerGroupDetails.getPartition()).get().getFirstOffset()
	   consumerPartition.setFirstOffset(topicForFirstOffset.get().getPartition(consumerGroupDetails.getPartition()).get().getFirstOffset());
	   consumerTopic.addOffset(consumerPartition);
	   
	   //Consumer section
		  // consumer = new ConsumerVO(groupId);
		   consumer.addTopic(consumerTopic);
	   }
	   //Consumer section
	  // consumer = new ConsumerVO(groupId);
	   //consumer.addTopic(consumerTopic);
	   
	   return Optional.ofNullable(consumer);
   }
   
   
   @Override
   public Optional<ConsumerVO> getConsumerByTopicName(String groupId, String topicName)
   {
      return getConsumerByTopic(groupId, Optional.of(topicName).flatMap(this::getTopic).orElse(null));
   }

   @Override
   public Optional<ConsumerVO> getConsumerByTopic(String groupId, TopicVO topic)
   {
      final ConsumerVO consumer = new ConsumerVO(groupId);
      final ZKGroupDirs groupDirs = new ZKGroupDirs(groupId);

      if (consumerTreeCache.getCurrentData(groupDirs.consumerGroupDir()) == null) return Optional.empty();

      // todo: get number of threads in each instance (subscription -> topic -> # threads)
      Optional.ofNullable(consumerTreeCache.getCurrentChildren(groupDirs.consumerRegistryDir()))
         .ifPresent(
            children ->
               children.keySet().stream()
                  .map(id -> readConsumerRegistration(groupDirs, id))
                  .forEach(consumer::addActiveInstance));

      Stream<String> topicStream = null;

      if (topic != null)
      {
         if (consumerTreeCache.getCurrentData(groupDirs.consumerGroupDir() + "/owners/" + topic.getName()) != null)
         {
            topicStream = Stream.of(topic.getName());
         }
         else
         {
            topicStream = Stream.empty();
         }
      }
      else
      {
         topicStream = Optional.ofNullable(
            consumerTreeCache.getCurrentChildren(groupDirs.consumerGroupDir() + "/owners"))
            .map(Map::keySet)
            .map(Collection::stream)
            .orElse(Stream.empty());
      }

      topicStream
         .map(ConsumerTopicVO::new)
         .forEach(consumerTopic -> {
            getConsumerPartitionStream(groupId, consumerTopic.getTopic(), topic)
               .forEach(consumerTopic::addOffset);
            consumer.addTopic(consumerTopic);
         });

      return Optional.of(consumer);
   }

   private ConsumerRegistrationVO readConsumerRegistration(ZKGroupDirs groupDirs, String id)
   {
      try
      {
         ChildData data = consumerTreeCache.getCurrentData(groupDirs.consumerRegistryDir() + "/" + id);
         final Map<String, Object> consumerData = objectMapper.reader(Map.class).readValue(data.getData());
         Map<String, Integer> subscriptions = (Map<String, Integer>) consumerData.get("subscription");

         ConsumerRegistrationVO vo = new ConsumerRegistrationVO(id);
         vo.setSubscriptions(subscriptions);
         return vo;
      }
      catch (IOException ex)
      {
         throw Throwables.propagate(ex);
      }
   }

   private Stream<ConsumerPartitionVO> getConsumerPartitionStream(String groupId,
                                                                  String topicName,
                                                                  TopicVO topicOpt)
   {
      ZKGroupTopicDirs groupTopicDirs = new ZKGroupTopicDirs(groupId, topicName);

      if (topicOpt == null || topicOpt.getName().equals(topicName))
      {
         topicOpt = getTopic(topicName).orElse(null);
      }

      if (topicOpt != null)
      {
         final TopicVO topic = topicOpt;

         Map<Integer, Long> consumerOffsets = getConsumerOffsets(groupId, topic);

         return topic.getPartitions().stream()
            .map(partition -> {
               int partitionId = partition.getId();

               final ConsumerPartitionVO consumerPartition = new ConsumerPartitionVO(groupId, topicName, partitionId);
               consumerPartition.setOwner(
                  Optional.ofNullable(
                     consumerTreeCache.getCurrentData(groupTopicDirs.consumerOwnerDir() + "/" + partitionId))
                     .map(data -> new String(data.getData()))
                     .orElse(null));

               consumerPartition.setOffset(consumerOffsets.getOrDefault(partitionId, -1L));

               final Optional<TopicPartitionVO> topicPartition = topic.getPartition(partitionId);
               consumerPartition.setSize(topicPartition.map(TopicPartitionVO::getSize).orElse(-1L));
               consumerPartition.setFirstOffset(topicPartition.map(TopicPartitionVO::getFirstOffset).orElse(-1L));

               return consumerPartition;
            });
      }
      else
      {
         return Stream.empty();
      }
   }

   private Map<Integer, Long> getConsumerOffsets(String groupId, TopicVO topic)
   {
      try
      {
         // Kafka doesn't really give us an indication of whether a consumer is
         // using Kafka or Zookeeper based offset tracking. So look up the offsets
         // for both and assume that the largest offset is the correct one.

         ForkJoinTask<Map<Integer, Long>> kafkaTask =
            threadPool.submit(() -> getConsumerOffsets(groupId, topic, false));

         ForkJoinTask<Map<Integer, Long>> zookeeperTask =
            threadPool.submit(() -> getConsumerOffsets(groupId, topic, true));

         Map<Integer, Long> zookeeperOffsets = zookeeperTask.get();
         Map<Integer, Long> kafkaOffsets = kafkaTask.get();
         zookeeperOffsets.entrySet()
            .forEach(entry -> kafkaOffsets.merge(entry.getKey(), entry.getValue(), Math::max));
         return kafkaOffsets;
      }
      catch (InterruptedException ex)
      {
         Thread.currentThread().interrupt();
         throw Throwables.propagate(ex);
      }
      catch (ExecutionException ex)
      {
         throw Throwables.propagate(ex.getCause());
      }
   }

   private Map<Integer, Long> getConsumerOffsets(String groupId,
                                                 TopicVO topic,
                                                 boolean zookeeperOffsets)
   {
      return retryTemplate.execute(
         context -> brokerChannel(zookeeperOffsets ? null : offsetManagerBroker(groupId))
            .execute(channel -> getConsumerOffsets(channel, groupId, topic, zookeeperOffsets)));
   }

   /**
    * Returns the map of partitionId to consumer offset for the given group and
    * topic. Uses the given blocking channel to execute the offset fetch request.
    *
    * @param channel          The channel to send requests on
    * @param groupId          Consumer group to use
    * @param topic            Topic to query
    * @param zookeeperOffsets If true, use a version of the API that retrieves
    *                         offsets from Zookeeper. Otherwise use a version
    *                         that pulls the offsets from Kafka itself.
    * @return Map where the key is partitionId and the value is the consumer
    * offset for that partition.
    */
   private Map<Integer, Long> getConsumerOffsets(BlockingChannel channel,
                                                 String groupId,
                                                 TopicVO topic,
                                                 boolean zookeeperOffsets)
   {

      final OffsetFetchRequest request = new OffsetFetchRequest(
         groupId,
         topic.getPartitions().stream()
            .map(p -> new TopicAndPartition(topic.getName(), p.getId()))
            .collect(Collectors.toList()),
         (short) (zookeeperOffsets ? 0 : 1), 0, // version 0 = zookeeper offsets, 1 = kafka offsets
         clientId());

      LOG.debug("Sending consumer offset request: {}", request);

      channel.send(request.underlying());

      final kafka.api.OffsetFetchResponse underlyingResponse =
    		  kafka.api.OffsetFetchResponse.readFrom(channel.receive().payload());
         //kafka.api.OffsetFetchResponse.readFrom(channel.receive().buffer());

      LOG.debug("Received consumer offset response: {}", underlyingResponse);

      OffsetFetchResponse response = new OffsetFetchResponse(underlyingResponse);

  /*    return response.offsets().entrySet().stream()
    	         .filter(entry -> entry.getValue().error().code() == ErrorMapping.NoError())
    	         .collect(Collectors.toMap(entry -> entry.getKey().partition(), entry -> entry.getValue().offset()));
      */
      return response.offsets().entrySet().stream()
         .filter(entry -> entry.getValue().error() == ErrorMapping.NoError())
         .collect(Collectors.toMap(entry -> entry.getKey().partition(), entry -> entry.getValue().offset()));
   }

   /**
    * Returns the broker Id that is the offset coordinator for the given group id. If not found, returns null
    */
   private Integer offsetManagerBroker(String groupId)
   {
      return retryTemplate.execute(
         context ->
            brokerChannel(null)
               .execute(channel -> offsetManagerBroker(channel, groupId))
      );
   }

   private Integer offsetManagerBroker(BlockingChannel channel, String groupId)
   {
/*      final ConsumerMetadataRequest request =
         new ConsumerMetadataRequest(groupId, (short) 0, 0, clientId());
*/	   
	   final GroupCoordinatorRequest request =
	         new GroupCoordinatorRequest(groupId, (short) 0, 0, clientId());


      LOG.debug("Sending consumer metadata request: {}", request);

      channel.send(request);
/*      ConsumerMetadataResponse response =
         ConsumerMetadataResponse.readFrom(channel.receive().buffer());
*/
      
      GroupCoordinatorResponse response =
    		  GroupCoordinatorResponse.readFrom(channel.receive().payload());
    		  //GroupCoordinatorResponse.readFrom(channel.receive().buffer());
      LOG.debug("Received consumer metadata response: {}", response);

      return (response.errorCode() == ErrorMapping.NoError()) ? response.correlationId(): null;
      //return (response.error().code() == ErrorMapping.NoError()) ? response.correlationId() : null;
   }

   private Map<Integer, Long> getTopicPartitionSizes(TopicVO topic)
   {
      return getTopicPartitionSizes(topic, kafka.api.OffsetRequest.LatestTime());
   }

   private Map<Integer, Long> getTopicPartitionSizes(TopicVO topic, long time)
   {
      try
      {
         PartitionOffsetRequestInfo requestInfo = new PartitionOffsetRequestInfo(time, 1);

         return threadPool.submit(() ->
               topic.getPartitions().parallelStream()
                  .filter(p -> p.getLeader() != null)
                  .collect(Collectors.groupingBy(p -> p.getLeader().getId())) // Group partitions by leader broker id
                  .entrySet().parallelStream()
                  .map(entry -> {
                     final Integer brokerId = entry.getKey();
                     final List<TopicPartitionVO> brokerPartitions = entry.getValue();
                     try
                     {
                        // Get the size of the partitions for a topic from the leader.
                        final OffsetResponse offsetResponse =
                           sendOffsetRequest(brokerId, topic, requestInfo, brokerPartitions);


                        // Build a map of partitionId -> topic size from the response
                        return brokerPartitions.stream()
                           .collect(Collectors.toMap(TopicPartitionVO::getId,
                                                     partition -> Optional.ofNullable(
                                                        offsetResponse.offsets(topic.getName(), partition.getId()))
                                                        .map(Arrays::stream)
                                                        .orElse(LongStream.empty())
                                                        .findFirst()
                                                        .orElse(-1L)));
                     }
                     catch (Exception ex)
                     {
                        LOG.error("Unable to get partition log size for topic {} partitions ({})",
                                  topic.getName(),
                                  brokerPartitions.stream()
                                     .map(TopicPartitionVO::getId)
                                     .map(String::valueOf)
                                     .collect(Collectors.joining(",")),
                                  ex);

                        // Map each partition to -1, indicating we got an error
                        return brokerPartitions.stream().collect(Collectors.toMap(TopicPartitionVO::getId, tp -> -1L));
                     }
                  })
                  .map(Map::entrySet)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .get();
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         throw Throwables.propagate(e);
      }
      catch (ExecutionException e)
      {
         throw Throwables.propagate(e.getCause());
      }
   }

   private OffsetResponse sendOffsetRequest(Integer brokerId, TopicVO topic,
                                            PartitionOffsetRequestInfo requestInfo,
                                            List<TopicPartitionVO> brokerPartitions)
   {
      final OffsetRequest offsetRequest = new OffsetRequest(
         brokerPartitions.stream()
            .collect(Collectors.toMap(
               partition -> new TopicAndPartition(topic.getName(), partition.getId()),
               partition -> requestInfo)),
         (short) 0, clientId());

      LOG.debug("Sending offset request: {}", offsetRequest);

      return retryTemplate.execute(
         context ->
            brokerChannel(brokerId)
               .execute(channel ->
                        {
                           channel.send(offsetRequest.underlying());
                           final kafka.api.OffsetResponse underlyingResponse =
                        		   kafka.api.OffsetResponse.readFrom(channel.receive().payload());
                              //kafka.api.OffsetResponse.readFrom(channel.receive().buffer());

                           LOG.debug("Received offset response: {}", underlyingResponse);

                           return new OffsetResponse(underlyingResponse);
                        }));
   }

   private class BrokerListener implements PathChildrenCacheListener
   {
      @Override
      public void childEvent(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception
      {
         switch (event.getType())
         {
            case CHILD_REMOVED:
            {
               BrokerVO broker = removeBroker(brokerId(event.getData()));
               break;
            }

            case CHILD_ADDED:
            case CHILD_UPDATED:
            {
               addBroker(parseBroker(event.getData()));
               break;
            }

            case INITIALIZED:
            {
               brokerPathCache.getCurrentData().stream()
                  .map(BrokerListener.this::parseBroker)
                  .forEach(CuratorKafkaMonitor.this::addBroker);
               break;
            }
         }
         updateController();
      }

      private int brokerId(ChildData input)
      {
         return Integer.parseInt(StringUtils.substringAfter(input.getPath(), ZkUtils.BrokerIdsPath() + "/"));
      }


      private BrokerVO parseBroker(ChildData input)
      {
         try
         {
            final BrokerVO broker = objectMapper.reader(BrokerVO.class).readValue(input.getData());
            broker.setId(brokerId(input));
            return broker;
         }
         catch (IOException e)
         {
            throw Throwables.propagate(e);
         }
      }
   }



   /* 
    * ORIGINAL
    *  
   @Override
   public List<ConsumerVO> getConsumers()
   {
      validateInitialized();
      return getConsumerStream(null).collect(Collectors.toList());
   }

   
   @Override
   public List<ConsumerVO> getConsumers(final TopicVO topic)
   {
      validateInitialized();
      return getConsumerStream(topic)
         .filter(consumer -> consumer.getTopic(topic.getName()) != null)
         .collect(Collectors.toList());
   }*/
   
/*   private class ConsumerGroup{
	   private String topic;
	   private int partition;
	   private int currentOffest;
	   private int logEndOffset;
	   private int lag;
	   private String consumerId;
	   private String host;
	   private String clientId;
	
	   
	   public ConsumerGroup(String topic, int partition, int currentOffest, int logEndOffset, int lag, String consumerId,
			String host, String clientId) {
		super();
		this.topic = topic;
		this.partition = partition;
		this.currentOffest = currentOffest;
		this.logEndOffset = logEndOffset;
		this.lag = lag;
		this.consumerId = consumerId;
		this.host = host;
		this.clientId = clientId;
	}

	   public ConsumerGroup(String[] allMembers) {
		   this.topic = allMembers[0];
		   this.partition = returnIntegerValue(allMembers[1]);
		   this.currentOffest = returnIntegerValue(allMembers[2]);
		   this.logEndOffset = returnIntegerValue(allMembers[3]);
		   this.lag = returnIntegerValue(allMembers[4]);
		   this.consumerId = allMembers[5];
		   this.host = allMembers[6];
		   this.clientId = allMembers[7];
	   }

	   
		
	private int returnIntegerValue(String string) {
       try {
           return Integer.valueOf(string).intValue();
       } catch (NumberFormatException e) {
           return -1;
       }
   }   
	
	
	public String getTopic() {
		return topic;
	}


	public int getPartition() {
		return partition;
	}


	public int getCurrentOffest() {
		return currentOffest;
	}


	public int getLogEndOffset() {
		return logEndOffset;
	}


	public int getLag() {
		return lag;
	}


	public String getConsumerId() {
		return consumerId;
	}


	public String getHost() {
		return host;
	}


	public String getClientId() {
		return clientId;
	}
	   
	public boolean isMatchTopics(String topic){
		return topic.equals(this.getTopic());
	}
   }
   */

   /*  private Map<String,ConsumerGroup> getAllOrSpecificConsumerGroupDescription(String groupId)
   {
	   String bootstrap = System.getProperty("bootstrap");
	   String command = System.getProperty("command");
	   
	   String groupDetailsCommand = new String(command+ " --new-consumer --bootstrap-server "+ bootstrap+ " --describe --group ");
	   //String groupDetailsCommandOld = new String(command+ " --bootstrap-server "+ bootstrap+ " --describe --group ");

	   String list;
	   Map<String,ConsumerGroup> result = new HashMap<String,ConsumerGroup>();
	   
	   //only for presenting the consumer-groups 
	   if (groupId == null){
		   final List<String>allGroupIdsList = getConsumersString();
		   for(int i=0; i<allGroupIdsList.size(); i++){
			   String prepForExec = allGroupIdsList.get(i);
			   List<String> commandList = execCommand(groupDetailsCommand+prepForExec);
			   //List<String> commandList = execCommand(groupDetailsCommandOld+prepForExec);
			   list = commandList.get(2);
			   result.put(allGroupIdsList.get(i), new ConsumerGroup(list.trim().split("\\s+")));			   
			}
	   }
	 //need to present all consumers under a consumer-group 
	   else{
		   List<String> list1 = execCommand(groupDetailsCommand+groupId);
		   //List<String> list1 = execCommand(groupDetailsCommandOld+groupId);
		   int j = 2;
			while (j < list1.size()) {
				list = list1.get(j);
				result.put(groupId+"_"+String.valueOf(j-1), new ConsumerGroup(list.trim().split("\\s+")));
				j++;
			}
		
	   }

	   return result;
   }
 */  
   /*   
   @Override
   public List<ConsumerVO> getConsumers(final TopicVO topic)
   {
	   List<ConsumerVO> result = new ArrayList<>();
	   Map<String,ConsumerGroup> groupDetailsPerRelevantTopic = new HashMap<String,ConsumerGroup>();
	   
	   groupDetailsPerRelevantTopic = getAllOrSpecificConsumerGroupDescription(null);
   
	   for (Map.Entry<String,ConsumerGroup> entry : groupDetailsPerRelevantTopic.entrySet())
	   {
			if(entry.getValue().isMatchTopics(topic.getName())){
				result.add(new ConsumerVO(entry.getKey()));
			}
			
		}
	
	   result.forEach(consumer->consumer.addTopic(new ConsumerTopicVO(topic.getName())));
	   return result;
   }
   
*/
   public void jmxManagement() throws Exception 
   {
	   JMXConnector jmxConn = null;
	   try {
		JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://illnqw1164.corp.amdocs.com:9096/jndi/rmi://illnqw1164.corp.amdocs.com:9096/jmxrmi");
		jmxConn = JMXConnectorFactory.connect(url, null);
		
		MBeanServerConnection mbsConn = jmxConn.getMBeanServerConnection();
		
		String objName = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions";
		
		ObjectName objectName = new ObjectName(objName);
		
		KafkaConsumer<String,String> kafkaConsumer = getKafkaConsumer("FWA");
		
		String attributeName = "Value";
		Object value = mbsConn.getAttribute(objectName, attributeName);
		
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	   finally {
		   if (jmxConn != null)
		   {
			   jmxConn.close(); 
		   }	  
	}
   }
}
