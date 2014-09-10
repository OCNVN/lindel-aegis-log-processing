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
public class DepartamentoAnuncioRHash {
    private Jedis jedis;
    
    public DepartamentoAnuncioRHash(Jedis jedis){
        this.jedis = jedis;
    }
    
    public boolean create(JSONObject anuncioObj){
        if(keyGenerator(anuncioObj) != null && anuncioObj.get("precio") != null && anuncioObj.get("titulo") != null && anuncioObj.get("_id") != null){
            jedis.hset(keyGenerator(anuncioObj) , "permalink", anuncioObj.get("permalink").toString());
            jedis.hset(keyGenerator(anuncioObj) , "precio", anuncioObj.get("precio").toString());
            jedis.hset(keyGenerator(anuncioObj) , "titulo", anuncioObj.get("titulo").toString());
            if(anuncioObj.get("foto_principal_url") != null)
                jedis.hset(keyGenerator(anuncioObj) , "foto_principal_url", anuncioObj.get("foto_principal_url").toString());
            jedis.hset(keyGenerator(anuncioObj) , "_id", anuncioObj.get("_id").toString());
        }
        
        return true;
    }
    
    private String keyGenerator(JSONObject anuncioObj){
        
        if(anuncioObj != null && anuncioObj.get("permalink") != null && anuncioObj.get("categoria") != null && ((JSONObject)anuncioObj.get("categoria")).get("permalink") != null ){
            JSONObject categoriaObj = (JSONObject) anuncioObj.get("categoria");
            
            return "RECOMENDACION:departamento:" + categoriaObj.get("_id") + ":anuncio:" + anuncioObj.get("_id");
        }
        
        return null;
    }

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }
      
}
