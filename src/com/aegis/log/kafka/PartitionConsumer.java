/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.log.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 *
 * @author carloslucero
 */
public class PartitionConsumer {
    private int particion;
    private String topic;
    private int puerto;
    private List<String> replicaBrokers;
    private List<String> brokersDisponibles;
    
    private PartitionMetadata partitionMetadata;
    
    // Consumer de Kafka javaapi
    private SimpleConsumer consumer = null;
    
    public PartitionConsumer(List<String> brokersDisponibles, int particion, String topic, int puerto) {
        this.particion = particion;
        this.topic = topic;
        this.puerto = puerto;
        this.brokersDisponibles = brokersDisponibles;
        
        replicaBrokers = new ArrayList<String>();
    }
    
    protected void buscarLider(List<String> brokersDisponibles) {
        for (String seed : brokersDisponibles) {
            SimpleConsumer consumer = null;
            try {
                // Configuramos un SimpleConsumer usando el brokerDisponible de turno
                consumer = new SimpleConsumer(seed, puerto, 100000, 64 * 1024, "leaderLookup");
                // Configuramos parametros para obtener metadatos del topic TopicMetadataRequest, al parecer se puede enviar varios topics a la vez
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                // Consultamos metadatos del topic
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
                // Obtenemos el metadata de la lista de TopicMetadata, solo debe haber uno ya que el TopicMetadataRequest se configuro con un solo topic
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    // Obtenemos los metadatos de las particiones del topic
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        // Encontramos la particion con el numero correspondiente a la variable this.particion
                        if (part.partitionId() == particion) {
                            partitionMetadata = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + particion + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        
        // Con el objeto de metadatos de la particion, obtenemos las replicas de la particion y guardamos en replicaBrokers
        if (partitionMetadata != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : partitionMetadata.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
    }

    // Host del broker lider
    private String leadBroker;
    // Nombre del cliente con el cual haremos la peticion del stream
    private String clientName;
    // Aqui es donde podemos configurar el ultimo mensaje leido para esta particion, podemos guardar en zookeeper el valor por ejemplo
    private long readOffset;
    
    public void init(){
        consumer = null;
                
        try{
            // find the meta data about the topic and partition we are interested in
            //
            buscarLider(brokersDisponibles);
            if (partitionMetadata == null) {
                System.out.println("Can't find metadata for Topic and Partition. Exiting");
                return;
            }
            if (partitionMetadata.leader() == null) {
                System.out.println("Can't find Leader for Topic and Partition. Exiting");
                return;
            }

            
            leadBroker = partitionMetadata.leader().host();
            clientName = "Client_" + topic + "_" + particion;

            System.out.println("LEAD BROKER: " + leadBroker + ", CLIENT_NAME: " + clientName);

            // Creamos el consumer que consumira el stream
            consumer = new SimpleConsumer(leadBroker, puerto, 100000, 64 * 1024, clientName);
            // Aqui es donde podemos configurar el ultimo mensaje leido para esta particion, podemos guardar en zookeeper el valor por ejemplo
            readOffset = getLastOffset(consumer,topic, particion, kafka.api.OffsetRequest.EarliestTime(), clientName);

        }catch(Exception ex){
            System.out.println("Error al intentar obtener Stream, causado por: " + ex);
        }finally{
            if (consumer != null) consumer.close();
        }
    }
    
    public List<String> read(){
        try{
            List<String> mensajesBuffer = new ArrayList<>();
            int numErrors = 0;
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, puerto, 100000, 64 * 1024, clientName);
            }

            FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, particion, readOffset, 100000)
                .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, particion);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                //if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,topic, particion, kafka.api.OffsetRequest.LatestTime(), clientName);
                    return null;
                }
                consumer.close();
                consumer = null;
                leadBroker = buscarNuevoLider(leadBroker);
                return null;
            }
            numErrors = 0;

            //long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, particion)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + "[" + particion + "]" + new String(bytes, "UTF-8"));
                
                mensajesBuffer.add(new String(bytes, "UTF-8"));
                //numRead++;
                //a_maxReads--;
            }

            return mensajesBuffer;
            /*if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) { // OJO! cuando hay reconexion a zookeeper esto lo hace para parar un instante hasta que termine de recuperarse el servicio
                }
            }*/
        }catch(Exception ex){
            //System.out.println("Error al intentar obtener Stream, causado por: " + ex);
            return null;
        }
    }
    
    private String buscarNuevoLider(String antiguoLider) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            buscarLider(replicaBrokers);
            if (partitionMetadata == null) {
                goToSleep = true;
            } else if (partitionMetadata.leader() == null) {
                goToSleep = true;
            } else if (antiguoLider.equalsIgnoreCase(partitionMetadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return partitionMetadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
    
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
