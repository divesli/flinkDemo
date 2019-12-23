package com.xy;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xy.config.HttpFactory;
import com.xy.config.flink.FrameKeyedSerializationSchema;
import com.xy.config.flink.FrameSchema;
import com.xy.config.flink.SimpleStringGenerator;
import com.xy.core.service.impl.TelemeteringDataAssortment;
import com.xy.eunms.TransferType;
import com.xy.eunms.UrlEnums;
import com.xy.protobuf.Any;
import com.xy.protobuf.InvalidProtocolBufferException;
import com.xy.utils.ApolloConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.xy.core.service.impl.TelemeteringDataAssortment.*;
import static com.xy.eunms.TopicEnums.COUNT_TIME_TOPIC;
import static com.xy.eunms.TopicEnums.SIMULATE_TELEMETERING;
import static com.xy.eunms.YbEnums.MESSAGE_CHANGE;

/**
 * @author cc
 */
@Slf4j
public class Go {

    /**
     * 发送时间队列
     */
    public static final ConcurrentLinkedQueue<String> TIME_QUEUE = new ConcurrentLinkedQueue<>();

    private static Properties properties = ApolloConfig.getInstance().toProperties();

    private static  final  MapStateDescriptor<String, Tuple2<Optional<List<JSONObject>>, Optional<Map<Long, Long>>>> YB_PARAM = new MapStateDescriptor<>(
            "YB_PARAM",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Tuple2<Optional<List<JSONObject>>, Optional<Map<Long, Long>>>>() {
            }));

    private static final String CODE = "code";

    public static void main(String[] args)  {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //发送统计时间
        timeStatistics(env);
        //配置文件变更
        BroadcastStream<ExchangeMessage.Frame> frameDataStream = ybConfigInit(env);
        //数据处理
        env.setParallelism(5);
        ybParamHandler(env, frameDataStream, Collections.singletonList(SIMULATE_TELEMETERING.getTopic()));

        try {
            env.execute("beach");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void timeStatistics(StreamExecutionEnvironment env) {
        env.addSource(new SimpleStringGenerator())
                .addSink(new FlinkKafkaProducer<>(COUNT_TIME_TOPIC.getTopic(), new SimpleStringSchema(), properties))
                .name("sendTime");
    }

    private static void ybParamHandler(StreamExecutionEnvironment env, BroadcastStream<ExchangeMessage.Frame> frameDataStream, List<String> topics) {
        FlinkKafkaConsumer<ExchangeMessage.Frame> kafkaConsumer = new FlinkKafkaConsumer<>(topics, new FrameSchema(), properties);
        kafkaConsumer.setStartFromLatest();

        env.addSource(kafkaConsumer).connect(frameDataStream).process(new BroadcastProcessFunction<ExchangeMessage.Frame, ExchangeMessage.Frame
                , Tuple2<ExchangeMessage.Frame, ExchangeMessage.Frame>>() {
            private TelemeteringDataAssortment telemeteringDataAssortment = initialize();
            boolean equals = topics.stream().anyMatch(data -> telemeteringDataAssortment.getTopicEnum().contains(data));
            @Override
            public void processElement(ExchangeMessage.Frame value, ReadOnlyContext ctx, Collector<Tuple2<ExchangeMessage.Frame
                    , ExchangeMessage.Frame>> out) throws Exception {
                if (!equals){
                    return;
                }
                try {
                    Tuple2<ExchangeMessage.Frame, ExchangeMessage.Frame> frameFrameTuple2 = telemeteringDataAssortment.messageProcess(value);
                    out.collect(frameFrameTuple2);
                } catch (Exception e) {
                    log.error("遥测数据解析失败, e" + e.getMessage());
                }
            }
            @Override
            public void processBroadcastElement(ExchangeMessage.Frame value, Context ctx, Collector<Tuple2<ExchangeMessage.Frame, ExchangeMessage.Frame>> out) throws Exception {
                log.info("云豹配置数据变更");
            }
        }).flatMap(new RichFlatMapFunction<Tuple2<ExchangeMessage.Frame, ExchangeMessage.Frame>, ExchangeMessage.Frame>() {
            @Override
            public void flatMap(Tuple2<ExchangeMessage.Frame, ExchangeMessage.Frame> value, Collector<ExchangeMessage.Frame> out) throws Exception {
                if (value == null){
                    return;
                }
                if (value.f0!=null){
                    out.collect(value.f0);
                }
                if (value.f1!=null){
                    out.collect(value.f1);
                }
            }
        }).addSink(new FlinkKafkaProducer<>(TransferType.ACTUAL.getResultTopic(), new FrameKeyedSerializationSchema(), properties))
                .name("sendParams");
    }

    private static BroadcastStream<ExchangeMessage.Frame> ybConfigInit(StreamExecutionEnvironment env) {
        return env.addSource(new FlinkKafkaConsumer<>(MESSAGE_CHANGE.getTopic(), new FrameSchema(), properties))
                .map(new RichMapFunction<ExchangeMessage.Frame, ExchangeMessage.Frame>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        syncYbParams();
                        syncTelemetryModel();
                    }
                    @Override
                    public ExchangeMessage.Frame map(ExchangeMessage.Frame value) throws Exception {
                        syncYbParams();
                        syncTelemetryModel();
                        return value;
                    }
                }).broadcast(YB_PARAM);
    }

    /**
     * 同步云豹数据
     * @return
     */
    private static void syncYbParams() {

        String s = HttpUtil.get(properties.getProperty("yb.address")+ UrlEnums.YB_PARAMS.getUrl());
        JSONObject jsonObject = JSONObject.parseObject(s);
        if (jsonObject.get(CODE).equals(HttpFactory.Status.SUCCESS.getCode())) {
            JSONArray data = jsonObject.getJSONArray("data");
            List<JSONObject> ybParamResult = getYbParamResult(data);
            if (!ybParamResult.isEmpty()) {
                ybParamList.clear();
                ybParamList.addAll(ybParamResult);
                log.info(String.format("更新云豹遥测参数, 更新条数 %s", ybParamResult.size()));
                return;
            }
        }
        log.error(String.format("云豹遥测遥测请求失败, 返回结果%s", jsonObject));
    }

    private static void syncTelemetryModel(){
        String post = HttpUtil.post(properties.getProperty("yb.address") + UrlEnums.YB_TELEMETRY_MODEL.getUrl(),
                new HashMap<>(100));
        JSONObject jsonObject =  JSONObject.parseObject(post);
        if (jsonObject.get(CODE).equals(HttpFactory.Status.SUCCESS.getCode())){
            JSONArray data = jsonObject.getJSONArray("data");
            Map<Long, Long> longLongMap = getLongLongMap(data);
            if (!longLongMap.isEmpty()){
                ybTelemetryModelMap.clear();
                ybTelemetryModelMap.putAll(longLongMap);
                log.info(String.format("更新云豹航天器识别字, 更新条数 %s", longLongMap.size()));
                return;
            }
        }
        log.error(String.format("云豹航天器识别字请求失败, 返回结果%s", jsonObject));
    }

}
