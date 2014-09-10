/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.stream.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.aegis.log.redis.DepartamentoAnuncioRHash;
import com.aegis.log.redis.DepartamentoAnuncioVisitasRSet;
import com.aegis.log.redis.DepartamentoAnuncioVisitasRSortSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

/**
 *
 * @author carloslucero
 */
public class AnuncioVisitasProcessingBolt extends BaseRichBolt{
    private OutputCollector collector;
    private Integer id;
    
    // Conexion a Redis
    Jedis jedis;
    
    private DepartamentoAnuncioRHash departamentoAnuncioRHash;
    private DepartamentoAnuncioVisitasRSortSet departamentoAnuncioVisitasRSortSet;
    private DepartamentoAnuncioVisitasRSet departamentoAnuncioVisitasRSet;
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        System.out.println("PREPARE BOLT ");
        this.collector = oc;
        this.id = tc.getThisTaskId();
        
        // Obtenemos una conexion a redis desde el pool
        jedis = AnuncioVisitasTopology.jedisPool.getResource();
        
        // Mapeo a la base redis
        departamentoAnuncioRHash = new DepartamentoAnuncioRHash(jedis);
        departamentoAnuncioVisitasRSortSet = new DepartamentoAnuncioVisitasRSortSet(jedis);
        departamentoAnuncioVisitasRSet = new DepartamentoAnuncioVisitasRSet(jedis);
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("[" + this.id + "]"+" (CotizacionNormalizerBolt recibe) " + tuple);
        
        Object value = JSONValue.parse(tuple.getString(0));
        
        JSONObject obj=(JSONObject)value;
        if(obj != null && obj.get("permalink") != null){
            System.out.println("LEYO: " + obj.get("permalink"));

            departamentoAnuncioRHash.create(obj);
            //departamentoAnuncioVisitasRSortSet.incr(obj);
            Long count = departamentoAnuncioVisitasRSet.add(obj);
            
            // almacenamos cuantas visitas existen para este anuncio
            System.out.println("VISITAS TOTALES " + count);
            if(obj.get("_id") != null && ((JSONObject)JSONValue.parse(obj.get("categoria").toString())).get("_id") != null)
                departamentoAnuncioVisitasRSortSet.add(
                        ((JSONObject)JSONValue.parse(obj.get("categoria").toString())).get("_id").toString(), 
                        count, 
                        obj.get("_id").toString());
            else
                System.out.println("ALGUN VALOR ES NULO POR ESO NO GUARDA CONTEO");
            
            //departamentoAnuncioVisitasRSet.cleanOldRecords();
            
            //jedis.set("FOOJAVA", "BARJAVA");
        }
        
        //map.putIfAbsent("323", obj);
        //map.remove("123");
        
        
        /*String mensaje = tuple.getString(0);
        
        String[] mensajeParts = mensaje.split(",");
        
        String permalink = mensajeParts[0];
        Double precio = new Double(mensajeParts[1]);
        Integer ticker = new Integer(mensajeParts[2]);
        String type = mensajeParts[3];
        Integer contador = new Integer(mensajeParts[4]);
        String timestamp = mensajeParts[5];

        // Pasar la tupla al siguiente bolt
        ArrayList<Tuple> a = new ArrayList<Tuple>();
        a.add(tuple);
        
        // Resultado de procesamiento
        //CotizacionPojo cotizacion = new CotizacionPojo(permalink, precio, ticker, type);
        HashMap cotizacion = new HashMap();
        cotizacion.put("permalink", permalink);
        cotizacion.put("precio", precio);
        cotizacion.put("ticker", ticker);
        cotizacion.put("type", type);
        cotizacion.put("contador", contador);
        cotizacion.put("timestamp", timestamp);
        
        //System.out.println("[" + this.id + "]" + "NORMALIZER " + cotizacion.get("contador"));
        System.out.println("[" + this.id + "]"+" (CotizacionNormalizerBolt emite) " + cotizacion);
        collector.emit(a, new Values(cotizacion));
        */
        /*String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            System.out.println("MENSAJE STREAM " + word);
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                
                List a = new ArrayList();
                a.add(tuple);
                collector.emit(a, new Values(word));
            }
        }
        
        collector.ack(tuple);*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("word"));
        
    }
    
}
