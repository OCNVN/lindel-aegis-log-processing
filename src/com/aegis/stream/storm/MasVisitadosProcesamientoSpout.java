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
import com.aegis.log.redis.DepartamentoAnuncioVisitasRSet;
import com.aegis.stream.conf.Conf;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;

/**
 *
 * @author carloslucero
 */
public class MasVisitadosProcesamientoSpout extends BaseRichSpout{
    private Integer id;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    
    // Conexion a Redis
    Jedis jedis;

    private DepartamentoAnuncioVisitasRSet departamentoAnuncioVisitasRSet;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("line"));
    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        this.collector = soc;
        this.context = tc;
        this.id = tc.getThisTaskId();
        
        // Obtenemos una conexion a redis desde el pool
        jedis = AnuncioVisitasTopology.jedisPool.getResource();
        
        departamentoAnuncioVisitasRSet = new DepartamentoAnuncioVisitasRSet(jedis);
    }

    @Override
    public void nextTuple() {
        calcularPeriodo();
        
        // A cambiado de periodo para recalcular?
        if(cambioPeriodo){
            System.out.println("CAMBIO DE PERIODO: " + periodoActual);
            // Obtenemos las claves de los sets que contienen los timestamp de las visitas por cada anuncio durante el periodo anterior
            Set<String> keys = departamentoAnuncioVisitasRSet.getAll();
            
            // Enviamos cada clave al siguiente bolt
            for (Iterator<String> it = keys.iterator(); it.hasNext();) {
                String key = it.next();
        
                HashMap tupla = new HashMap();
                tupla.put("key", key);
                tupla.put("periodo_actual", periodoActual);
                tupla.put("fixed_timestamp", fixedTimestamp);
                
                this.collector.emit(new Values(tupla), tupla);
                //this.collector.emit(new Values(key), key);
            }
        }
    }
    
    private int periodoActual = -1;
    private long fixedTimestamp = -1;
    private boolean cambioPeriodo = false;
    
    public void calcularPeriodo(){
        Long timestamp = new Date().getTime();
        
        //System.out.println("TIMESTAMP: " + timestamp);
        Calendar calFechaHora = new GregorianCalendar();
        
        int year = calFechaHora.get(Calendar.YEAR);
        int month = calFechaHora.get(Calendar.MONTH);
        int dayOfMonth = calFechaHora.get(Calendar.DAY_OF_MONTH);
        int hour = calFechaHora.get(Calendar.HOUR);
        
        Calendar calFecha = new GregorianCalendar(year, month, dayOfMonth, 0, 0);
        fixedTimestamp = calFecha.getTime().getTime();
        
        //System.out.println("TIMESTAMP FIXED: " + timestampFixed);
        
        Long timestampDiff = timestamp - fixedTimestamp;
        int periodo = (int) (timestampDiff/Conf.LAPSO_REFRESCO_TIMESTAMP);
        
        if(periodoActual == -1)
            periodoActual = periodo;
        else if(periodoActual != periodo){
            periodoActual = periodo;
            cambioPeriodo = true;
        }else if(periodoActual == periodo)
            cambioPeriodo = false;
        
        //System.out.println("PERIODOS " + periodo);
    }
}
