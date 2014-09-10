/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.stream.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.aegis.log.kafka.TopicConsumerMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class AnuncioVisitasTopology {
    // Informacion del Topic a consumir
    String topic = "anuncios.visitas.partition";
    List<String> brokersDisponibles = new ArrayList<String>();
    int port = 9092;
    
    TopicConsumerMetadata metadata = null;
    
    List<PartitionConsumerSpout> partitionConsumersSpout = new ArrayList<>();
    
    // Pool de conexion a redis
    public static JedisPool jedisPool;
    
    
    public AnuncioVisitasTopology(){
        // Agregamos un servidor a la lista de brokers disponibles
        brokersDisponibles.add("localhost");
        
        // Obtener metadatos del topic a consumir
        metadata = new TopicConsumerMetadata(topic, brokersDisponibles, port);
        
        // Conexion a redis
        jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
    }
    
    private void iniciarConsumidores(){
        // El for recorre el numero de particiones desde 0 hasta el numero de particiones del metadata
        for(int i = 0; i < metadata.getNumParticiones(); i++){
            PartitionConsumerSpout pcs = new PartitionConsumerSpout();
            //pcs.setTopicConsumerMetadata(metadata);
            pcs.setParticion(i);
            pcs.setBrokersDisponibles(metadata.getBrokersDisponibles());
            pcs.setPuerto(metadata.getPuerto());
            pcs.setTopic(metadata.getTopic());
            partitionConsumersSpout.add(pcs);
        }
    }
    
    private void iniciarTopologia(){
        iniciarConsumidores();
        
        // Arranque de topology
        TopologyBuilder builder = new TopologyBuilder();
        
        System.out.println("AGREGANDO SPOUTS A LA TOPOLOGIA");
        for(int i = 0; i < partitionConsumersSpout.size(); i++)
            builder.setSpout("spout-anuncio-visitas-partition_" + i, partitionConsumersSpout.get(i));
        
        System.out.println("AGREGANDO BOLTS A LA TOPOLOGIA");
        for(int i = 0; i < partitionConsumersSpout.size(); i++)
            builder.setBolt("bolt-anuncio-visitas-processing-partition_" + i, new AnuncioVisitasProcessingBolt(), 2).shuffleGrouping("spout-anuncio-visitas-partition_" + i);
        
        builder.setSpout("spout-anuncio-visitas-calculo", new MasVisitadosProcesamientoSpout());
        builder.setBolt("bolt-visitas-timestamp-cleaner", new VisitasTimestampCleanerBolt(), 2).shuffleGrouping("spout-anuncio-visitas-calculo");
        
        //builder.setSpout("cotizacion-reader",new CotizacionesStreamSpout()); 
        //builder.setBolt("cotizacion-normalizer", new CotizacionNormalizerBolt(), 8).shuffleGrouping("cotizacion-reader"); 
        //builder.setBolt("transacciones-entities", new TransaccionesEntidadesBolt(), 32).shuffleGrouping("cotizacion-normalizer");
        
        //Configuration 
        Config conf = new Config(); 
        conf.setDebug(false);
        //Topology run 
        conf.setNumWorkers(8);

        //StormSubmitter.submitTopology("genium-topology", conf, builder.createTopology());
        
        LocalCluster cluster = new LocalCluster();
        System.out.println("SUBMIT TOPOLOGY");
        cluster.submitTopology("anuncios-visitas-topology", conf, builder.createTopology());
        //cluster.killTopology("Getting-Started-Toplogie");
        //cluster.shutdown();
        try {
            Thread.sleep(600000);
        } catch (InterruptedException ex) {
            Logger.getLogger(AnuncioVisitasTopology.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    //private PartitionConsumer partitionConsumer;
    
    public static void main(String [] args) {
        AnuncioVisitasTopology topology = new AnuncioVisitasTopology();
        topology.iniciarTopologia();
    }
}
