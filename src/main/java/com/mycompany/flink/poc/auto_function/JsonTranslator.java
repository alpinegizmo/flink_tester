package com.mycompany.flink.poc.auto_function;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.sling.commons.json.JSONObject;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class JsonTranslator extends RichMapFunction<String, JSONObject> {

	private static final long serialVersionUID = 1L;
	private AtomicInteger counter; 
    transient StatsDClient stats;
    final String statsdAspect = "a_json_translator";
    final String statsdUrl; 
   
    public JsonTranslator(String statsdUrl) {
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
	public JSONObject map(String str) throws Exception {
      JSONObject obj = new JSONObject(str);
	  stats.increment(statsdAspect); 
  	  return obj;
	}

}
