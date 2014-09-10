/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.log.redis;

import com.aegis.stream.conf.Conf;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

/**
 *
 * @author carloslucero
 */
public class DepartamentoAnuncioVisitasRSet {
    private Jedis jedis;
    
    public DepartamentoAnuncioVisitasRSet(Jedis jedis){
        this.jedis = jedis;
    }
    
    public Long add(JSONObject anuncioObj){
        if(keyGenerator(anuncioObj) != null && anuncioObj.get("precio") != null && anuncioObj.get("titulo") != null && anuncioObj.get("_id") != null && anuncioObj.get("visita_timestamp") != null){
            Long resultado = jedis.sadd(keyGenerator(anuncioObj), anuncioObj.get("visita_timestamp").toString());
            
            if(resultado == 1)
                return jedis.scard(keyGenerator(anuncioObj));
        }
        
        return -1l;
    }
    
    public Long rem(String key, Long timestamp){
         jedis.srem(key, timestamp.toString());
        return jedis.scard(key);
    }
    
    public Set<String> members(String key){
        Set<String> members = jedis.smembers(key);
        
        return members;
    }
    
    public Set<String> getAll(){
        Set<String> keys = jedis.keys("RECOMENDACION:departamento:*:anuncio:*:visitas");
        
        return keys;
    }
    
    /*public void cleanOldRecords(){
        Set<String> keys = jedis.keys("RECOMENDACION:departamento:*:anuncio:*:visitas");
        
        for (Iterator<String> it = keys.iterator(); it.hasNext();) {
            String key = it.next();
            
            Set<String> members = jedis.smembers(key);
            
            for (Iterator<String> it1 = members.iterator(); it1.hasNext();) {
                String member = it1.next();
                
                Date fecha = new Date(Long.parseLong(member));
                System.out.println("[" + key + "] " + member + " = " + fecha);
                
            }
        }
    }*/
    
    private String keyGenerator(JSONObject anuncioObj){
        
        if(anuncioObj != null && anuncioObj.get("permalink") != null && anuncioObj.get("categoria") != null && ((JSONObject)anuncioObj.get("categoria")).get("permalink") != null ){
            JSONObject categoriaObj = (JSONObject) anuncioObj.get("categoria");
            
            return "RECOMENDACION:departamento:" + categoriaObj.get("_id") + ":anuncio:" + anuncioObj.get("_id") + ":visitas";
        }
        
        return null;
    }
    
    public String departamentoIdFromKey(String key){
        String pattern = "RECOMENDACION:departamento:";
        String departamentoId = key.replaceAll(pattern, "");
        pattern = "([^<]*):anuncio:([^<]*):visitas";
        departamentoId = departamentoId.replaceAll(pattern, "$1");
        
        return departamentoId;
    }
    
     public String anuncioIdFromKey(String key){
        String pattern = "RECOMENDACION:departamento:";
        String departamentoId = key.replaceAll(pattern, "");
        pattern = "([^<]*):anuncio:([^<]*):visitas";
        departamentoId = departamentoId.replaceAll(pattern, "$2");
        
        return departamentoId;
    }

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }
    
}
