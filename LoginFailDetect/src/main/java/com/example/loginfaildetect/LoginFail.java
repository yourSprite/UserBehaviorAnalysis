package com.example.loginfaildetect;

import com.example.loginfaildetect.beans.LoginEvent;
import com.example.loginfaildetect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author yutian
 * @version 1.0
 * @date 2021/8/29
 */
public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        assert resource != null;
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent) {
                        return loginEvent.getTimestamp() * 1000L;
                    }
                });

        // 自定义处理函数检测连续登陆失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute();

    }

    // 实现自定义KeyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 定义属性，最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态：保存2s内所有的登陆失败事件
        ListState<LoginEvent> loginFailEventListState;


        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前事件登陆状态
            if ("fail".equals(loginEvent.getLoginState())) {
                // 1.如果是登陆失败，获取状态中之前的登陆失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果已有登陆失败事件，继续判断是否在2s内
                    // 获取已有的登陆失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (loginEvent.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        // 1.1.1 如果在2s之内，输出报警
                        collector.collect(new LoginFailWarning(loginEvent.getUserId(), firstFailEvent.getTimestamp(),
                                loginEvent.getTimestamp(), "login fail 2 times in 2s"));
                    }

                    // 不管报不报警，这次都已处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(loginEvent);
                } else {
                    // 1.2 如果没有登陆失败，直接将当前事件存入liststate
                    loginFailEventListState.add(loginEvent);
                }
            } else {
                // 2.如果是登陆成功，直接清空状态
                loginFailEventListState.clear();
            }
        }
    }
}
