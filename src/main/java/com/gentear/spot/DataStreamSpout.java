package com.gentear.spot;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
//spout 译为 n. 喷口；水龙卷；水落管；水柱
public class DataStreamSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;

    private int index = 0;

    //以下三个方法为继承BaseRichSpout  其中BaseRichSpout默认实现了部分方法
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        //出始化方法
        System.out.println("+++++open+++++");
        this.collector = collector;
    }

    @Override
    public void nextTuple(){

        //该方法循环调用
        System.out.println("+++++nextTuple+++++");

        ++ index;
        if (index > 10){
            //计算完成提交value值
            collector.emit(new Values("resoultValue"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        //告诉bolt传递的key值
        declarer.declare(new Fields("resoultKey"));

        System.out.println("+++++declareOutputFields+++++");
    }
}
