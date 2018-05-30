package com.mycompany.flink.poc.auto_function;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.sling.commons.json.JSONObject;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class PassthruFunction extends RichMapFunction<JSONObject, JSONObject> {
    private AtomicInteger counter; 
    transient StatsDClient stats;
    final String statsdAspect = "a_finalizer_passthru";
    final String statsdUrl; 
   
    public PassthruFunction(String statsdUrl) {
    	 this.statsdUrl = statsdUrl;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        counter = new AtomicInteger(); 
        stats = new NonBlockingStatsDClient("A_", statsdUrl.split(":")[0], Integer.parseInt(statsdUrl.split(":")[1]));
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        try {
            stats.stop();
        } catch (Exception e) {
        }
    }
    
    @Override
	public JSONObject map(JSONObject value) throws Exception { 
	  stats.increment(statsdAspect); 
  	  return value;
	}

}