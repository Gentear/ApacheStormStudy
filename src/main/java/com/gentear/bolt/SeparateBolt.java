package com.gentear.bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class SeparateBolt extends BaseRichBolt{
    //继承 BaseRichBolt 实现以下两个方法 同spout

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        //初始化调用
        System.out.println("++++prepare++++");
    }

    @Override
    public void execute(Tuple input){
        if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            //由 getComponentConfiguration 方法定义的时间
            //指定的时间执行该方法
            System.out.println("每30秒执行一次");
        }else {
            //该方法循环调用 input中获取spout中传过来的值
            Fields fields = input.getFields();
            String resoult = fields.get(0);
            System.out.println(resoult);
            System.out.println("++++execute++++");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        //同spout中的 declareOutputFields 提交时放入key值
        System.out.println("++++declareOutputFields++++");
    }

    //可以通过重写下面方法获取config
    @Override
    public Map<String, Object> getComponentConfiguration() {
        //可以通过下面的方法  每30s持久化一次数据
        Map<String, Object> conf = new HashMap<String, Object>(16);
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.parseInt("30"));
        return conf;

    }

}
