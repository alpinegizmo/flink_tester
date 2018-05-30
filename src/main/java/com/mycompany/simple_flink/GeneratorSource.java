package com.mycompany.simple_flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

public class GeneratorSource extends RichParallelSourceFunction<String> {
    private static Logger logger = Logger.getLogger(GeneratorSource.class);
    private volatile boolean run = false; 
    private int loop = 0;

	public GeneratorSource(int loop) {
		this.loop = loop;
	}
    
    @Override
    public void open(Configuration parameters){
        run = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
    	int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    	int eachPartitionRec = loop/getRuntimeContext().getNumberOfParallelSubtasks();

    	int upperBound = (subTaskIndex + 1) * eachPartitionRec;
    	int lowerbound = upperBound - eachPartitionRec;
        while (run){
        	String idx = String.format("%09d", lowerbound);
        	//logger.info("{\"#input_id\":\"1524817180x001_001121\",\"#output_id\":\"\",\"#input_type\":\"FX\",\"#output_type\":\"FX\",\"#addkey\":\"\",\"#source_id\":\"FXAGG1\",\"#filename\":\"USG03420170804143031KK_HUSA_PGW_KK_PGW2_20170804_143008_664966.dat\",\"I_GPRSRECORDTYPE\":\"GP3\",\"I_RECORDID\":\"2040571556               \",\"I_POINT_ORIGIN\":\"853F                 \",\"I_GUIDE_NUMBER_TYPE\":\"14   \",\"I_TRANSACTIONID\":\"79A0A6A4\",\"I_SEQUENCE_NUMBER\":\"01\",\"I_BILL_CLASS\":\"0    \",\"I_PROVIDER_ID\":\"4000 \",\"I_DATE\":\"20170804142942\",\"I_PRIMARY_UNITS\":\"9         \",\"I_SECONDARY_UNITS\":\"9         \",\"I_TERTIARY_UNITS\":\"16        \",\"I_PRERATED_AMT\":\"          \",\"I_PRERATED_TAX\":\"          \",\"I_PRERATED_CC\":\" \",\"I_COMPLETION_STATUS\":\"0  \",\"I_TIMEZONE\":\"10\",\"I_DIALLED_DIGITS\":\"403airtelgprs.com        \",\"I_CDR_TYPE\":\"GP3       \",\"I_CHARGED_PARTY\":\"0\",\"I_DISCONNECTING_PARTY\":\"0\",\"I_RADIO_CHANNI_USED\":\"1\",\"I_SERVICE_CODE\":\"11\",\"I_MSC_ID\":\"              \",\"I_RELATED_NUMBER\":\"                     \",\"I_IMEI\":\"3584250712202301     \",\"I_FIRST_CELL\":\"                \",\"I_LAST_CELL\":\"                \",\"I_ROUTE\":\"          \",\"I_SEIZURE_TIME\":\"142942\",\"I_CAC\":\"BTSol     \",\"I_PDP_ADDRESS\":\"100.102.104.67 \",\"I_PDP_TYPE\":\"121 \",\"I_COS\":\"0\",\"I_DIFF_QOS\":\"0    \",\"I_TTFILENAME\":\"KK_HUSA.KK_PGW2_20170804_143008_664966                                          \",\"PARTIAL_TYPE\":\"A\",\"ROUND_UP_PRIM_UNITS\":\"10\",\"ROUND_UP_SEC_UNITS\":\"10\",\"ROUND_UP_TERT_UNITS\":\"16        \",\"I_USAGE_TYPE\":\"1002904\",\"I_IMSI\":\"404450915054802      \",\"I_MSISDN\":\"" + idx + "\",\"UNIQUE_KEY\":\"A_" + idx + "_1002904_403airtelgprs.com\",\"FILE_SEQ_NUM\":\"664966\",\"I_POINT_TARGET\":\"403airtelgprs.com\",\"SEPARATION_FIELD\":\"A_FX12_000000\"}");
        	ctx.collect("{\"#input_id\":\"1524817180x001_001121\",\"#output_id\":\"\",\"#input_type\":\"FX\",\"#output_type\":\"FX\",\"#addkey\":\"\",\"#source_id\":\"FXAGG1\",\"#filename\":\"USG03420170804143031KK_HUSA_PGW_KK_PGW2_20170804_143008_664966.dat\",\"I_GPRSRECORDTYPE\":\"GP3\",\"I_RECORDID\":\"2040571556               \",\"I_POINT_ORIGIN\":\"853F                 \",\"I_GUIDE_NUMBER_TYPE\":\"14   \",\"I_TRANSACTIONID\":\"79A0A6A4\",\"I_SEQUENCE_NUMBER\":\"01\",\"I_BILL_CLASS\":\"0    \",\"I_PROVIDER_ID\":\"4000 \",\"I_DATE\":\"20170804142942\",\"I_PRIMARY_UNITS\":\"9         \",\"I_SECONDARY_UNITS\":\"9         \",\"I_TERTIARY_UNITS\":\"16        \",\"I_PRERATED_AMT\":\"          \",\"I_PRERATED_TAX\":\"          \",\"I_PRERATED_CC\":\" \",\"I_COMPLETION_STATUS\":\"0  \",\"I_TIMEZONE\":\"10\",\"I_DIALLED_DIGITS\":\"403airtelgprs.com        \",\"I_CDR_TYPE\":\"GP3       \",\"I_CHARGED_PARTY\":\"0\",\"I_DISCONNECTING_PARTY\":\"0\",\"I_RADIO_CHANNI_USED\":\"1\",\"I_SERVICE_CODE\":\"11\",\"I_MSC_ID\":\"              \",\"I_RELATED_NUMBER\":\"                     \",\"I_IMEI\":\"3584250712333301     \",\"I_FIRST_CELL\":\"                \",\"I_LAST_CELL\":\"                \",\"I_ROUTE\":\"          \",\"I_SEIZURE_TIME\":\"142942\",\"I_CAC\":\"BTSol     \",\"I_PDP_ADDRESS\":\"110.102.104.67 \",\"I_PDP_TYPE\":\"121 \",\"I_COS\":\"0\",\"I_DIFF_QOS\":\"0    \",\"I_TTFILENAME\":\"BB_HUSA.KK_PGW2_20170814_143008_664966                                          \",\"PARTIAL_TYPE\":\"A\",\"ROUND_UP_PRIM_UNITS\":\"10\",\"ROUND_UP_SEC_UNITS\":\"10\",\"ROUND_UP_TERT_UNITS\":\"16        \",\"I_USAGE_TYPE\":\"1002904\",\"I_IMSI\":\"404450915054802      \",\"I_MSISDN\":\"" + idx + "\",\"UNIQUE_KEY\":\"A_" + idx + "_1002904_403airtelgprs.com\",\"FILE_SEQ_NUM\":\"664966\",\"I_POINT_TARGET\":\"403airtelgprs.com\",\"SEPARATION_FIELD\":\"A_FX12_000000\"}");
        	lowerbound++;
        	if(lowerbound == upperBound) 
        		run = false;
        }
    }

    @Override
    public void cancel() {
        run = false;
    }
}
