package com.mycompany.simple_flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONObject;

import com.mycompany.flink.poc.auto_function.AggregrationFunction;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;;

public class ProcessAggregation extends ProcessFunction<JSONObject, JSONObject> {
    private ReducingState<JSONObject> mergedRecordStore;
    private int aggrWindowsIntervalMs;
    final String statsdAspect = "a_process_aggregation";
    final String statsdAspect2 = "a_process_aggregation_timer";
    final String statsdUrl; 
    transient StatsDClient stats;
    
    ProcessAggregation(int aggrWindowsIntervalMs, String statsdUrl) {
    	this.aggrWindowsIntervalMs = aggrWindowsIntervalMs;
    	this.statsdUrl = statsdUrl;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        stats = new NonBlockingStatsDClient("A_", statsdUrl.split(":")[0], Integer.parseInt(statsdUrl.split(":")[1]));
        mergedRecordStore = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(
    			"reducing record store", new ReduceFunction<JSONObject>() {
    				@Override
    				public JSONObject reduce(JSONObject r1, JSONObject r2) throws Exception {
    					return new AggregrationFunction().reduce(r1, r2);
    				}
    			}, JSONObject.class));
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
    public void processElement(JSONObject r, Context ctx, Collector<JSONObject> out)
            throws Exception {
    		mergedRecordStore.add(r);
        	ctx.timerService().registerProcessingTimeTimer(((ctx.timerService().currentProcessingTime() + aggrWindowsIntervalMs) / 1000) * 1000);
    		stats.increment(statsdAspect);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out)
            throws Exception {
    	JSONObject output = mergedRecordStore.get();
    	out.collect(output);
    	mergedRecordStore.clear();
    	stats.increment(statsdAspect2); 
    }
}        
