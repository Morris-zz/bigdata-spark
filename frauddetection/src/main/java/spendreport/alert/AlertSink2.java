package spendreport.alert;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * @author: zhaozheng
 * @date: 2022/9/20 2:48 下午
 * @description:
 */
@PublicEvolving
public class AlertSink2 implements SinkFunction<Alert2> {
}
