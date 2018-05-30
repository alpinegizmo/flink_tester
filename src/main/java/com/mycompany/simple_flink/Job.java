package com.mycompany.simple_flink;
 
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;

import com.mycompany.flink.poc.auto_function.AggregrationFunction;
import com.mycompany.flink.poc.auto_function.JsonTranslator;
import com.mycompany.flink.poc.auto_function.PassthruFunction;

public class Job {
    public static Logger logger = Logger.getLogger(Job.class);
	public static void main(String[] args) throws Exception {
		String statsdUrl = "192.168.56.200:8127";
		int loop = 10;
		int aggrDuration = 10000;
        for(String arg : args) {
            String[] splited = arg.split("=");
            switch(splited[0]) {
                case "URL":
                	statsdUrl = splited[1];
                    System.out.println("URL is updated to: " + statsdUrl);
                    break;
                case "loop":
                	loop = Integer.parseInt(splited[1]);
                    System.out.println("loop is updated to: " + loop);
                    break;
                case "aggrinterval":
                	aggrDuration = Integer.parseInt(splited[1]);
                    System.out.println("aggrinterval is updated to: " + aggrDuration);
                    break;
            }
        }
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> stream = env.addSource(new GeneratorSource(loop)).name("GeneratorSource: " + loop);
        DataStream<JSONObject> convert = stream.map(new StatsdMeter()).name("StatsDMeter")
        									.map(new JsonTranslator(statsdUrl)).name("JsonTranslator")
        									.keyBy(new KeySelector<JSONObject, AggregationKey>() {
			@Override
			public AggregationKey getKey(JSONObject r) throws Exception {
				AggregationKey ak = new AggregationKey();
				ak.setMsisdn(r.getString("I_MSISDN"));
				ak.setUsageType(r.getString("I_USAGE_TYPE"));
				ak.setPointTarget(r.getString("I_POINT_TARGET"));
				return ak;
			}
		}).process(new ProcessAggregation(aggrDuration, statsdUrl)).name("AggregationDuration: " + aggrDuration +"ms")
        									.map(new PassthruFunction(statsdUrl)).name("finalizerPassthru");  
		env.execute("Flink Java API Skeleton");
	} 
}