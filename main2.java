package com.xy;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.xy.conf.ApplicationConfig;
import com.xy.conf.MessageTypeEnum;
import com.xy.conf.PropertiesWord;
import com.xy.conf.serialization.FrameSchema;
import com.xy.conf.serialization.JobNoticeSchema;
import com.xy.conf.serialization.BroadcastDataSchema;
import com.xy.conf.serialization.MessageBeanSchema;
import com.xy.logic.*;
import com.xy.model.*;
import com.xy.pack.Packer;
import com.xy.protobuf.Any;
import com.xy.protobuf.util.Timestamps;
import com.xy.redis.RedisClient;
import com.xy.util.Consts;
import com.xy.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.xy.util.PBUtil.print;
import static com.xy.util.PBUtil.unpack;


/**
 * @author Administrator
 * createTime 2019/10/10
 */
@Slf4j
public class StartMeasureApplication {
    /**
     * 定义广播流数据结构
     * Map<tid+sid, Map<time,TheoreticalMeasure.Data>>>
     */
    private static final MapStateDescriptor<String, Tuple2<Optional<Map<IdentifterWord, TreeMap<Long,TheoreticalMeasure.Data>>>,Optional<String>>> THEORYMEASURE = new MapStateDescriptor<>(
            "theoryMeasure",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Tuple2<Optional<Map<IdentifterWord, TreeMap<Long,TheoreticalMeasure.Data>>>,Optional<String>>>() {})
    );
    // 配置信息
    private static Properties properties = ApplicationConfig.getInstance().getProperties();

    // 空间目标编码与航天器识别字关系维护表
    private static Map<String, Long> spacecraftMap = SpacecraftInfoParse.getSpacecraftNode(properties.getProperty(PropertiesWord.CORE_URL));

    // 设备代号与设备地址编码之间的关系
    private static Map<String, Long> deviceMap = SpacecraftInfoParse.getDeviceNode(properties.getProperty(PropertiesWord.CORE_URL));

    // 帧错误记录队列
    public static final LinkedBlockingQueue<MessageBean> MESSAGE_QUEUE = new LinkedBlockingQueue<>();

    /**
     * 监听调度信息,获取外测数据,向定轨软件发送请求参数,通知其生成外测理论值
     * @param env
     */
    public static void listeningTaskInfo(StreamExecutionEnvironment env) {
        // 调度信息数据源
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer<>("job_notice", new JobNoticeSchema(), properties);
        DataStreamSource<JobSysPbData.JobSysPbDataBean> stream = env.addSource(flinkKafkaConsumer);
        stream.map(new RichMapFunction<JobSysPbData.JobSysPbDataBean, ExternalMeasurementDataPb.ExternalMeasurementDataBean>() {
            @Override
            public ExternalMeasurementDataPb.ExternalMeasurementDataBean map(JobSysPbData.JobSysPbDataBean jobSysPbDataBean) {
                JobSysPbData.SchedulingNotice notice = jobSysPbDataBean.getNotice();
                if (notice.getMissionType().equals("PrepareMission")) {
                    ExternalMeasurementDataPb.ParameterBody.Builder parameterBuilder = ExternalMeasurementDataPb.ParameterBody.newBuilder();
                    JSONObject object = JSONObject.parseObject(notice.getParamJson());
                    String noradId = object.getString("noradId");
                    String startTime = object.getString("startTime");
                    String endTime = object.getString("endTime");
                    String deviceCode = object.getString("devCd");
                    parameterBuilder.setSpatialEncoding(noradId); // 空间编目
                    parameterBuilder.setBeginTime(startTime); // 起始时间
                    parameterBuilder.setEndTime(endTime); // 结束时间
                    parameterBuilder.setDeviceName(deviceCode); // 设备名称
                    ExternalMeasurementDataPb.PredictDataBody.Builder predictDataBuilder = ExternalMeasurementDataPb.PredictDataBody.newBuilder();
                    predictDataBuilder.setDataStr("");
                    return ExternalMeasurementDataPb.ExternalMeasurementDataBean.newBuilder().setParameter(parameterBuilder.build()).setPredictData(predictDataBuilder.build()).build();
                }
                return null;
            }
        }).filter(input -> input != null)
                .addSink(new FlinkKafkaProducer<>("external_measurement_data", new JobNoticeSchema(), properties))
                .name("GetTheoreticalMeasureData");

    }

    /**
     * 发送误帧数据
     * @param env
     */
    private static void sendErrorStatisticsMessage(StreamExecutionEnvironment env) {
        env.addSource(new SimpleMessageGenerator())
                .addSink(new FlinkKafkaProducer<>("measure_message", new MessageBeanSchema(), properties))
                .name("sendMeasureMessage");
    }

    /**
     * 获取广播数据结构中的第一层map的key值。
     * 目前已航天器识别字作为第一层的key,因为目前一颗星同一个时间内只会被一个设备跟踪
     * @param spacecraftId
     * @return
     */
    private static IdentifterWord getIdentifterWord(Long spacecraftId, Long sourceId) {
        return new IdentifterWord(spacecraftId, sourceId);
    }

    /**
     * 获取Redis中的缓存数据
     * @param key
     * @return
     */
    private static Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>> getRedisCache(RedisClient redisClient, String key) {
        // 获取redis client
        try {
            String jsonStr = redisClient.getBroadcastStateTuple1(key);
            if (jsonStr != null && jsonStr.length() > 0) {
                JSONObject object = JSONObject.parseObject(jsonStr);
                Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>> dataMap = new HashMap<>();
                Iterator iter = object.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    JSONObject kObject = (JSONObject) entry.getKey();
                    Map<Long, JSONObject> data = JSONObject.parseObject(entry.getValue().toString(),
                            new TypeReference<Map<Long, JSONObject>>() {});
                    TreeMap<Long, TheoreticalMeasure.Data> treeMap = new TreeMap<>();
                    data.forEach((t, b) -> {
                        treeMap.put(t, JSONObject.parseObject(b.toString(), TheoreticalMeasure.Data.class));
                    });
                    dataMap.put(new IdentifterWord(kObject.getLong("spacecraftId"),kObject.getLong("sourceId")), treeMap);
                }
                return Optional.of(dataMap);
            }
        } catch (Exception e) {
            log.warn("Open failed.", e);
        }
        return Optional.empty();
    }

    public static void main(String[] args) throws Exception {

        // 定义执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(Integer.parseInt(properties.getProperty(PropertiesWord.PARALLELISM_NUMBER)));
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));
        // 设置checkpoint间隔1000ms
        //env.enableCheckpointing(1000);

        // 监听调度信息,跟踪开始时从轨道获取外测数据理论值
        listeningTaskInfo(env);

        // 发送错误帧统计
        sendErrorStatisticsMessage(env);

        RedisClient redisClient = new RedisClient(properties);

        // 定义理论外测理论数据广播流数据源,(需要将理论数据分发到每个节点上,故使用广播流)
        DataStreamSource<Tuple2<String, byte[]>> measureStream = env.addSource(
                new FlinkKafkaConsumer<>(Arrays.asList("external_measurement_data_result","message_topic"), new BroadcastDataSchema(), properties));

        // 广播流数据解析处理,监听外测理论值topic和配置信息变更topic
       BroadcastStream<Tuple2<Optional<Map<IdentifterWord,TreeMap<Long,TheoreticalMeasure.Data>>>, Optional<String>>> broadcastStream =
               measureStream.map(new MapFunction<Tuple2<String, byte[]>, Tuple2<Optional<Map<IdentifterWord,TreeMap<Long,TheoreticalMeasure.Data>>>, Optional<String>>>() {
                    @Override
                    public Tuple2<Optional<Map<IdentifterWord,TreeMap<Long,TheoreticalMeasure.Data>>>, Optional<String>> map(Tuple2<String, byte[]> tuple2) {
                        try {
                            String topic = tuple2.f0;
                            // 监听设备或航天器信息更新
                            if (topic.equals("message_topic")) {
                                Message.MsgBean msgBean =  Message.MsgBean.getDefaultInstance().getParserForType().parseFrom(tuple2.f1);
                                if (msgBean.getType() == MessageTypeEnum.NODE_INFO.getCode()
                                    || msgBean.getType() == MessageTypeEnum.DEVICE_INFO.getCode()) {
                                    return Tuple2.of(Optional.empty(),Optional.of(MessageTypeEnum.DEVICE_INFO.getMessage()));
                                } else if (msgBean.getType() == MessageTypeEnum.SPACECRAFT.getCode()
                                        || msgBean.getType() == MessageTypeEnum.SPACECRAFT_DEVICE.getCode()) {
                                    return Tuple2.of(Optional.empty(),Optional.of(MessageTypeEnum.SPACECRAFT.getMessage()));
                                }
                                return Tuple2.of(Optional.empty(),Optional.empty());
                            } else if (topic.equals("external_measurement_data_result")) {
                                ExternalMeasurementDataPb.ExternalMeasurementDataBean bean = ExternalMeasurementDataPb.ExternalMeasurementDataBean.parseFrom(tuple2.f1);
                                // 解析xml数据
                                JAXBContext context = JAXBContext.newInstance(TheoreticalMeasure.class);
                                Unmarshaller u = context.createUnmarshaller();
                                TheoreticalMeasure measureData = (TheoreticalMeasure) u.unmarshal(new StringReader(bean.getPredictData().getDataStr()));

                                Map<IdentifterWord,TreeMap<Long,TheoreticalMeasure.Data>> dataMap = new HashMap<>();
                                TreeMap<Long, TheoreticalMeasure.Data> map = new TreeMap<>();
                                if (measureData.getData() == null) {
                                    log.warn("理论值数据为空,measureData:" + measureData.toString());
                                    return Tuple2.of(Optional.empty(),Optional.empty());
                                }
                                measureData.getData().forEach(data -> {
                                    map.put(TimeUtil.convertDateToLong(data.getTime()),data);
                                });
                                Long spacecraftId = spacecraftMap.get(measureData.getSatellite().getSatID().trim());
                                Long sourceId = deviceMap.get(measureData.getSatellite().getStationName().trim());
                                dataMap.put(getIdentifterWord(spacecraftId,sourceId),map);

                                return Tuple2.of(Optional.of(dataMap),Optional.empty());
                            }
                        } catch (Exception e) {
                            log.warn("解析理论值数据失败", e);
                        }
                        return Tuple2.of(Optional.empty(),Optional.empty());
                    }
                }).filter(input -> input.f0.isPresent() || input.f1.isPresent())
                       .returns(TypeInformation.of(new TypeHint<Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>, Optional<String>>>(){}))
                       .name("BroadcastProcessing").broadcast(THEORYMEASURE);


        // 外测数据源
        DataStreamSource<ExchangeMessage.Frame> dataStream = env.addSource(new FlinkKafkaConsumer<>(Arrays.asList(properties.getProperty(PropertiesWord.RECEIVE_TOPIC).split(","))
                        , new FrameSchema(), properties));

        // 外测数据结果推送kafka producer
        FlinkKafkaProducer<ExchangeMessage.Frame> myProducer = new FlinkKafkaProducer<>(
                properties.getProperty(PropertiesWord.SEND_TOPIC),                  // target topic
                new FrameSchema(), properties);

        // 外测数据处理逻辑
        dataStream.connect(broadcastStream).process(
                new BroadcastProcessFunction<ExchangeMessage.Frame, Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>,Optional<String>>, ExchangeMessage.Frame>() {
                    private static final String key = "measure";
                    private volatile Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>,Optional<String>> tuple2 = null;
                    // 程序加载时(只执行一次)
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        log.warn("开始加载理论数据");
                        Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>> optionalMap = getRedisCache(redisClient, key);
                        if (optionalMap.isPresent()) {
                            log.warn("初始理论数据为: " + optionalMap.get().toString());
                            this.tuple2 = Tuple2.of(optionalMap,Optional.empty());
                        }
                    }
                    // 收到数据时(每条数据都执行)
                    @Override
                    public void processElement(ExchangeMessage.Frame frame,
                                               ReadOnlyContext ctx,
                                               Collector<ExchangeMessage.Frame> out) {
                        Any any = null;
                        try {
                            Tuple2<Optional<Map<IdentifterWord ,TreeMap<Long, TheoreticalMeasure.Data>>>,Optional<String>> tuple = ctx.getBroadcastState(THEORYMEASURE).get(key);

                            if (tuple == null) {
                                tuple = this.tuple2;
                            } else {
                                this.tuple2 = tuple;
                            }

                            Optional<ExchangeMessage.Origin> originOptional = unpack(frame.getPayload(), ExchangeMessage.Origin.class);
                            if (!originOptional.isPresent()) {
                                log.warn("数据解析错误:" + print(frame.getHead()).orElse("无法格式化数据!"));
                                MESSAGE_QUEUE.offer(MessageBean.builder()
                                        .spacecraftWord(frame.getHead().getSpacecraft())
                                        .source(frame.getHead().getSource())
                                        .bid(frame.getHead().getType())
                                        .errorCount(1).build());
                                out.collect(frame);
                            }
                            IdentifterWord k = getIdentifterWord(frame.getHead().getSpacecraft(),frame.getHead().getSource());

                            Optional<TreeMap<Long, TheoreticalMeasure.Data>> optionalMeasureMap = Optional.empty();
                            if (tuple != null && tuple.f0.isPresent() && tuple.f0.get() != null && tuple.f0.get().containsKey(k)) {
                                optionalMeasureMap = Optional.of(tuple.f0.get().get(k));
                            }

                            if (frame.getHead().getType() == Consts.ANGLE_BID) {
                                any = parseAngleData(originOptional, frame.getHead(),optionalMeasureMap);
                            } else if (frame.getHead().getType() == Consts.DISTANCE_BID) {
                                any = parseDistanceData(originOptional, frame.getHead(),optionalMeasureMap);
                            } else if (frame.getHead().getType() == Consts.SPEED_BID) {
                                any = parseSpeedData(originOptional, frame.getHead(),optionalMeasureMap);
                            }
                        } catch (Exception e) {
                            MESSAGE_QUEUE.offer(MessageBean.builder()
                                    .spacecraftWord(frame.getHead().getSpacecraft())
                                    .source(frame.getHead().getSource())
                                    .bid(frame.getHead().getType())
                                    .errorCount(1).build());
                            log.warn("数据解析错误:" + print(frame.getHead()).orElse("无法格式化数据!"), e);
                        }
                        if (any == null) {
                            out.collect(frame);
                        } else {
                            out.collect(frame.toBuilder().setPayload(any).build());
                        }
                    }
                    // 收到广播通知时执行
                    @Override
                    public void processBroadcastElement(final Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>,Optional<String>> tuple2, Context ctx, final Collector<ExchangeMessage.Frame> out) throws Exception {
                        try {
                            if (tuple2 != null) {
                                // 理论数据更新
                                if (tuple2.f0.isPresent()) {
                                    BroadcastState<String, Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>, Optional<String>>> broadcastState = ctx.getBroadcastState(THEORYMEASURE);
                                    Tuple2<Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>>, Optional<String>> oldTuple2 = broadcastState.get(key);
                                    // 之前是否已有理论数据,如果有则累加,没有则新增,key相同时则覆盖
                                    if (oldTuple2 == null || !oldTuple2.f0.isPresent()) {
                                        broadcastState.put(key, tuple2);
                                        Optional<Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>>> optionalOldCache = getRedisCache(redisClient, key);
                                        Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>> dataMap;
                                        if (optionalOldCache.isPresent()) {
                                            dataMap = optionalOldCache.get();
                                            dataMap.putAll(tuple2.f0.get());
                                        } else {
                                            dataMap = tuple2.f0.get();
                                        }
                                        redisClient.saveBroadcastStateTuple1(key, JSONObject.toJSONString(dataMap));
                                    } else {
                                        Map<IdentifterWord, TreeMap<Long, TheoreticalMeasure.Data>> dataMap = oldTuple2.f0.get();
                                        dataMap.putAll(tuple2.f0.get());
                                        broadcastState.put(key, Tuple2.of(Optional.of(dataMap), Optional.empty()));
                                        redisClient.saveBroadcastStateTuple1(key, JSONObject.toJSONString(dataMap));
                                    }
                                    log.warn("理论值更新");
                                }
                                // 航天器或设备相关配置数据更新
                                if (tuple2.f1.isPresent()) {
                                    if (tuple2.f1.get().equals(MessageTypeEnum.SPACECRAFT.getMessage())) {
                                        spacecraftMap = SpacecraftInfoParse.getSpacecraftNode(properties.getProperty(PropertiesWord.CORE_URL));
                                        log.warn("航天器信息更新");
                                    } else if (tuple2.f1.get().equals(MessageTypeEnum.DEVICE_INFO.getMessage())) {
                                        deviceMap = SpacecraftInfoParse.getDeviceNode(properties.getProperty(PropertiesWord.CORE_URL));
                                        log.warn("设备信息更新");
                                    }
                                }
                            }
                        } catch (Exception e) {
                            log.warn("理论值更新失败" , e);
                        }
                    }
                }).name("DataProcessing").addSink(myProducer);
        env.execute("flinkWCDataPrase");
    }


    /**
     * 处理测距数据
     * @param originOptional
     * @param head
     * @return
     */
    public static Any parseDistanceData(Optional<ExchangeMessage.Origin> originOptional, ExchangeMessage.Head head, Optional<TreeMap<Long, TheoreticalMeasure.Data>> optionalMeasureMap) throws Exception {

        byte[] bytes = originOptional.get().getValue().toByteArray();
        List<Object> list = Packer.unpack("HIqqH", bytes);
        char[] charsStatus = Packer.byteToBitChars(Packer.subBytes(bytes,0,2));
        DistanceStatusBean bean = DistanceStatusParse.getInstance().parseDistanceStatus(charsStatus);

        ExchangeMessage.Distance0x00100601.Builder builder = ExchangeMessage.Distance0x00100601.newBuilder();
        builder.setChannel(bean.getChannel())
                .setModel(bean.getModel())
                .setMixFrequency(bean.getMixFrequency())
                .setTtcRate(bean.getTtcRate())
                .setMeasureNum(bean.getMeasureNum())
                .setMixFrequency(bean.getMeasureFrequency())
                .setFm(bean.getFm())
                .setCm(bean.getCm())
                .setDistance(Packer.o2Long(list.get(2)))
                .setSampleRate(bean.getSampleRate())
                .setSpectrum(bean.getSpectrum())
                .setSnr(Packer.o2Integer(list.get(4)))
                .setTime(Timestamps.fromMillis(TimeUtil.getUtcTime(head.getTime().getSeconds(),Packer.o2Long(list.get(1)))));
        if (bean.getFm() == ExchangeMessage.FrequencyModel.SPREADSPECTRUMTWO) {
            builder.setDeltT(Packer.o2Long(list.get(3)));
        } else {
            builder.setDeltT(0);
        }
        MeasureOcParse.setDistanceOc(optionalMeasureMap, builder);
        //构建返回结果
        Any any = Any.pack(builder.build());
        return any;
    }

    /**
     * 处理测速数据
     * @param originOptional
     * @param head
     * @return
     */
    public static Any parseSpeedData(Optional<ExchangeMessage.Origin> originOptional, ExchangeMessage.Head head, Optional<TreeMap<Long, TheoreticalMeasure.Data>> optionalMeasureMap)  throws Exception {

        byte[] bytes = originOptional.get().getValue().toByteArray();
        // TODO 目前按16个字节处理,保留位置没有用到
        List<Object> list;
        int length = bytes.length;
        if (length == 16) {
            list = Packer.unpack("HIiih", bytes);
        } else {
            list = Packer.unpack("HIiiih", bytes);
        }
        char[] charsStatus = Packer.byteToBitChars(Packer.subBytes(bytes,0,2));
        SpeedStatusBean bean = SpeedStatusParse.getInstance().parseSpeedStatus(charsStatus);

        ExchangeMessage.Speed0x00100603.Builder builder = ExchangeMessage.Speed0x00100603.newBuilder();
        builder.setChannel(bean.getChannel())
                .setModel(bean.getModel())
                .setFm(bean.getFm())
                .setSampleCycle(bean.getSampleCycle())
                .setSpeedCalculus(bean.getSpeedCalculus())
                .setSpeed(Packer.o2Integer(list.get(2)))
                .setSpectrum(bean.getSpectrum())
                .setTime(Timestamps.fromMillis(TimeUtil.getUtcTime(head.getTime().getSeconds(),Packer.o2Long(list.get(1)))));
        if (length == 16) {
            builder.setSnr(Packer.o2Integer(list.get(4)));
        } else {
            builder.setSnr(Packer.o2Integer(list.get(5)));
        }


        if (bean.getFm() == ExchangeMessage.FrequencyModel.SPREADSPECTRUMTWO) {
            if (length == 16) {
                builder.setFrequencyDiff(Packer.o2Integer(list.get(3)));
            } else {
                builder.setFrequencyDiff(Packer.o2Integer(list.get(4)));
            }
        }
        MeasureOcParse.setSpeedOc(optionalMeasureMap, builder);
        //构建返回结果
        Any any = Any.pack(builder.build());
        return any;
    }

    /**
     * 处理测角数据
     * @param originOptional
     * @param head
     * @return
     */
    public static Any parseAngleData(Optional<ExchangeMessage.Origin> originOptional, ExchangeMessage.Head head,Optional<TreeMap<Long, TheoreticalMeasure.Data>> optionalMeasureMap)  throws Exception {
        byte[] bytes = originOptional.get().getValue().toByteArray();
        List<Object> list = Packer.unpack("BIiihhhhh", bytes);
        AngleStatusBean bean = AngleStatusParse.getInstance().parseAngleStatus(Packer.byteToBitChars(Packer.subBytes(bytes,0,1)));

        ExchangeMessage.Angle0x00100605.Builder builder = ExchangeMessage.Angle0x00100605.newBuilder();
        builder.setChannel(bean.getChannel())
                .setModel(bean.getModel())
                .setSampleRate(bean.getSampleRate())
                .setA(Packer.o2Integer(list.get(2)))
                .setE(Packer.o2Integer(list.get(3)))
                .setDeltA1(Packer.o2Integer(list.get(4)))
                .setDeltE1(Packer.o2Integer(list.get(5)))
                .setDeltA2(Packer.o2Integer(list.get(6)))
                .setDeltE2(Packer.o2Integer(list.get(7)))
                .setSnr(Packer.o2Integer(list.get(8)))
                .setSpectrum(bean.getSpectrum())
                .setTime(Timestamps.fromMillis(TimeUtil.getUtcTime(head.getTime().getSeconds(),Packer.o2Long(list.get(1)))));
        MeasureOcParse.setAngleOc(optionalMeasureMap, builder);
        //构建返回结果
        Any any = Any.pack(builder.build());
        return any;
    }
}
