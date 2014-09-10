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
import com.aegis.log.redis.DepartamentoAnuncioVisitasRSet;
import com.aegis.log.redis.DepartamentoAnuncioVisitasRSortSet;
import com.aegis.stream.conf.Conf;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;

/**
 *
 * @author carloslucero
 */
public class VisitasTimestampCleanerBolt extends BaseRichBolt{
    private OutputCollector collector;
    private Integer id;
    
    // Conexion a Redis
    Jedis jedis;
    
    private DepartamentoAnuncioVisitasRSet departamentoAnuncioVisitasRSet;
    private DepartamentoAnuncioVisitasRSortSet departamentoAnuncioVisitasRSortSet;
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        System.out.println("PREPARE BOLT ");
        this.collector = oc;
        this.id = tc.getThisTaskId();
        
        // Obtenemos una conexion a redis desde el pool
        jedis = AnuncioVisitasTopology.jedisPool.getResource();
        
        // Mapeo a la base redis
        departamentoAnuncioVisitasRSet = new DepartamentoAnuncioVisitasRSet(jedis);
        departamentoAnuncioVisitasRSortSet = new DepartamentoAnuncioVisitasRSortSet(jedis);
    }

    @Override
    public void execute(Tuple tuple) {
        //String setKey = tuple.getString(0);
        HashMap tupla = (HashMap) tuple.getValue(0); 
        System.out.println("[" + this.id + "] PROCESAR TIMESTAMPS DE KEY: " + tupla.get("key"));
        
        Set<String> members = departamentoAnuncioVisitasRSet.members(tupla.get("key").toString());
        
        // El periodo anterior lo obtenemos restando 1 al periodo actual
        Long periodoAnterior = Long.parseLong(tupla.get("periodo_actual").toString()) - 1;
            
        // Timestamp actual
        Long currentTimestamp = (new Date()).getTime();
        // Timestamp del periodo anterior
        Long lastTimestamp = Long.parseLong(tupla.get("fixed_timestamp").toString()) 
                + (periodoAnterior * Conf.LAPSO_REFRESCO_TIMESTAMP );
            
        System.out.println("[" + this.id + "] CUR_TIM : " + currentTimestamp);
        System.out.println("[" + this.id + "] LAS_TIM : " + lastTimestamp);
        
        departamentoAnuncioVisitasRSet.departamentoIdFromKey(tupla.get("key").toString());
        
        for (Iterator<String> it = members.iterator(); it.hasNext();) {
            // Timestamp del set obtenido de Redis
            Long memberTimestamp = new Long(it.next());
            
            // Eliminamos los timestamp de las visitas fuera del limite del calculo
            if(memberTimestamp < lastTimestamp){
                Long count = departamentoAnuncioVisitasRSet.rem(tupla.get("key").toString(), memberTimestamp);
                System.out.println("CONTADOR: " + count);
                departamentoAnuncioVisitasRSortSet.add(
                        departamentoAnuncioVisitasRSet.departamentoIdFromKey(tupla.get("key").toString()), 
                        count, 
                        departamentoAnuncioVisitasRSet.anuncioIdFromKey(tupla.get("key").toString()));
            }
                
            System.out.println("[" + this.id + "] MEMBER  : " + memberTimestamp);
        }
        
        try {
            System.out.println("PREPARANDO EJECUTAR LUA");
                    
            RandomAccessFile f;
            f = new RandomAccessFile("lua-scripts/departamento-anuncios-visitas-toplist.lua", "r");
            byte[] b = new byte[(int)f.length()];
            f.read(b);

            Integer numArgs = 1;
            String args = departamentoAnuncioVisitasRSet.departamentoIdFromKey(tupla.get("key").toString());
            jedis.eval(b, numArgs.byteValue(), args.getBytes());
                    
            System.out.println("EJECUTADO LUA");
            //jedis.eval(b, null, null);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(VisitasTimestampCleanerBolt.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(VisitasTimestampCleanerBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("word"));
        
    }
    
}
