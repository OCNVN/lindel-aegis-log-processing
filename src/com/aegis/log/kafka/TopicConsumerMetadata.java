/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.log.kafka;

import java.util.ArrayList;
import java.util.List;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 *
 * @author carloslucero
 */
public class TopicConsumerMetadata {
    // Numero de particiones en este pool
    private int numParticiones;
    
    private String topic;
    private List<String> brokersDisponibles;
    private int puerto;
    
    public TopicConsumerMetadata(String topic, List<String> brokersDisponibles, int puerto) {
        this.topic = topic;
        this.brokersDisponibles = brokersDisponibles;
        this.puerto = puerto;
        
        this.consultarNumParticiones();
        System.out.println("Numero de particiones: " + numParticiones);
    }
    
    /*
     * Consulta el numero de particiones que conforman el Topic
     */
    protected void consultarNumParticiones(){
        // Recorremos la lista de brokers disponibles
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
                    numParticiones = item.partitionsMetadata().size();
                }
            } catch (Exception e) {
                System.out.println("Error comunicando con Broker [" + seed + "] para encontrar Lider para [" + topic + "] Razon: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
    }
    
    public int getNumParticiones(){
        return numParticiones;
    }
    
    /*public void iniciarConsumidores(){
        for(int i = 0; i < this.numParticiones; i++){
            PartitionConsumer partitionConsumer = new PartitionConsumer(brokersDisponibles, i, topic, puerto);
            Thread thread = new Thread(partitionConsumer);
            
            thread.start();
            this.partitionConsumers.add(partitionConsumer);
        }
    }*/

    public String getTopic() {
        return topic;
    }

    public List<String> getBrokersDisponibles() {
        return brokersDisponibles;
    }

    public int getPuerto() {
        return puerto;
    }
}
