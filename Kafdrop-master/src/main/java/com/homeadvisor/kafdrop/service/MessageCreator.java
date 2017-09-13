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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Service
public class MessageCreator
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   @Autowired
   private KafkaMonitor kafkaMonitor;

   public String addMessageToTopic(String topicName, String msg)
   {
	  // final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
	   if(msg.isEmpty() || msg.trim().length() <= 0){
		   return "record was not sent because your message value is empty";
	   }
	   final Producer<Long, String> producer = createProducer();
	   long time = System.currentTimeMillis();
	  	   
	   try{
		   final ProducerRecord<Long, String> record =
	               new ProducerRecord<>(topicName, time,
	                           msg);
		   RecordMetadata metadata = producer.send(record).get();
		   
		   long elapsedTime = System.currentTimeMillis() - time;
           return "sent record(key= " + record.key() + " value= "+ record.value() +
        		   ") meta(partition= "+ metadata.partition() +", offset= " +metadata.offset()+ ") time= " + elapsedTime;
   
	   } catch (InterruptedException | ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	} finally {
	          producer.flush();
	          producer.close();
	   }

   }

   private Producer<Long, String> createProducer() {
	   final String BOOTSTRAP_SERVERS = kafkaMonitor.getBootstrap();
       Properties props = new Properties();
       props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                           BOOTSTRAP_SERVERS);
       props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
       props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    		   						LongSerializer.class.getName());
       props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                   StringSerializer.class.getName());
       return new KafkaProducer<>(props);
   }
}
