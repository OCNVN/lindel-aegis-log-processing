/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.stream.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.aegis.log.kafka.PartitionConsumer;
import com.aegis.log.kafka.TopicConsumerMetadata;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author carloslucero
 */
public class PartitionConsumerSpout extends BaseRichSpout{
    private Integer id;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    
    public PartitionConsumer partitionConsumer;
    
    // Metadatos del topic
    //public TopicConsumerMetadata topicConsumerMetadata;
    private String topic;
    private List<String> brokersDisponibles;
    private int puerto;
    public int particion;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("line"));
    }
    
    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        this.context = tc;
        this.collector = soc;
        this.id = tc.getThisTaskId();
        
        partitionConsumer= new PartitionConsumer(brokersDisponibles, particion, topic, puerto);
        partitionConsumer.init();
        System.out.println("PARTITION CONSUMER SPOUT INIT");
        
        // Logger.getLogger(CotizacionesStreamSpout.class.getName()).log(Level.SEVERE, null, ex);
    }

    @Override
    public void nextTuple() {
        // System.out.println("NEXT TUPLE");
        List<String> mensajesBuffer = partitionConsumer.read();
        if(mensajesBuffer != null && mensajesBuffer.size() > 0){
            System.out.println("RECIBE MENSAJE" + mensajesBuffer.size());
            
            for (Iterator<String> it = mensajesBuffer.iterator(); it.hasNext();) {
                String mensaje = it.next();

                this.collector.emit(new Values(mensaje), mensaje);
            }
        }
    }

    /*public void setTopicConsumerMetadata(TopicConsumerMetadata topicConsumerMetadata) {
        this.topicConsumerMetadata = topicConsumerMetadata;
    }*/

    public void setParticion(int particion) {
        this.particion = particion;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBrokersDisponibles(List<String> brokersDisponibles) {
        this.brokersDisponibles = brokersDisponibles;
    }

    public void setPuerto(int puerto) {
        this.puerto = puerto;
    }
    
}
