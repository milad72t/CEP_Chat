// milad teimouri 95725127
// run : .\flink.bat run E:\courses\master\term2\DistributedSystem\CEP\Windows\flink\target\flink-1.0-SNAPSHOT.jar
// .\nc.exe -l -p 9000
// Get-Content flink-milad72t-jobmanager-MILADD.out

package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import javax.xml.stream.FactoryConfigurationError;


public class ChatTracker {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> chatPatternStream = env
                .socketTextStream("localhost", 9000)
                .map(new ChatSplitter()) // convert string format to tuple
                .keyBy(0) // key by sender and receiver of message (Sender->Receiver)
//                .countWindow(3,1)
                .flatMap(new PatternTracker()); // check every 3 message by same sender and receiver for detect specifc pattern

        chatPatternStream.print();
        env.execute("CEP chat tracker By Milad");
    }

    public static class ChatSplitter implements
            MapFunction<String, Tuple2<String,String>> {
        public Tuple2<String,String> map(String sentence)
                throws Exception {
            String string = sentence;
            String[] parts = string.split("#");
            if (parts.length == 2){
                return Tuple2.of( parts[0], parts[1]);
            }
            return Tuple2.of( "Unkwon", "message");
        }
    }


    public static class PatternTracker extends RichFlatMapFunction<Tuple2<String,String>,String>{
        private transient ValueState<Integer> FlagPattern; // flag for maintain of state

        public void flatMap(Tuple2<String , String> input , Collector<String> out)
                throws Exception {
                    Integer State = FlagPattern.value(); // current state
                    if (input.f1.equals("salam")){
                        FlagPattern.update(1);
                        out.collect(String.format("%s , message : %s",input.f0,input.f1));
                    }
                    else if(input.f1.equals("khoubi") && State==1){
                        FlagPattern.update(2);
                        out.collect(String.format("%s , message : %s",input.f0,input.f1));
                    }
                    else if(input.f1.equals("chetouri") && State==2){
                        FlagPattern.update(0);
                        out.collect(String.format("Warning!! pattern detect between %s !!",input.f0));
                    }
                    else{
                        FlagPattern.update(0);
                        out.collect(String.format("%s , message : %s",input.f0,input.f1));
                    }
        }

        public void open(Configuration config){
            ValueStateDescriptor<Integer> descriptor=
                    new ValueStateDescriptor<Integer>(
                            "FlagPatternDetector" ,
                            TypeInformation.of(new TypeHint<Integer>() {}),
                            Integer.valueOf(0) // first initiate of flag
                    );
            FlagPattern = getRuntimeContext().getState(descriptor);
        }
    }
}
