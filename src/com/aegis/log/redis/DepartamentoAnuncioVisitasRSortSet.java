/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.aegis.log.redis;

import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

/**
 *
 * @author carloslucero
 */
public class DepartamentoAnuncioVisitasRSortSet {
    private Jedis jedis;
    
    public DepartamentoAnuncioVisitasRSortSet(Jedis jedis){
        this.jedis = jedis;
    }
    
    /*public Double incr(JSONObject anuncioObj){
        if(keyGenerator(anuncioObj) != null && anuncioObj.get("precio") != null && anuncioObj.get("titulo") != null && anuncioObj.get("_id") != null){
            return jedis.zincrby(keyGenerator(anuncioObj), 1, anuncioObj.get("_id").toString());
        }
        
        return -1d;
    }*/
    
    public Long add(String departamentoId, Long count, String anundioIid){
        return jedis.zadd(keyGenerator(departamentoId), Double.parseDouble(count.toString()), anundioIid);
    }
    
    private String keyGenerator(JSONObject anuncioObj){
        
        if(anuncioObj != null && anuncioObj.get("categoria") != null && ((JSONObject)anuncioObj.get("categoria")).get("permalink") != null ){
            JSONObject categoriaObj = (JSONObject) anuncioObj.get("categoria");
            
            return "RECOMENDACION:departamento:" + categoriaObj.get("_id") + ":visitas:conteo";
        }
        
        return null;
    }
    
    private String keyGenerator(String departamentoId){
        return "RECOMENDACION:departamento:" + departamentoId + ":visitas:conteo";
    }

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }
    
}
