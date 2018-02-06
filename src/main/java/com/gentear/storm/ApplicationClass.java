package com.gentear.storm;

import com.gentear.bolt.SeparateBolt;
import com.gentear.spot.DataStreamSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Time;

public class ApplicationClass{

    private final static String spout_id = "spout";

    private final static String separateBolt_id = "separateBolt";


    public static void main(String[] args) throws InterruptedException{


        LocalCluster localCluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();
        //spout
        DataStreamSpout spout = new DataStreamSpout();
        //分隔bolt
        SeparateBolt separateBolt = new SeparateBolt();

        builder.setSpout(spout_id,spout);
        //shuffleGrouping 设置广播形式 有不同种
        builder.setBolt(separateBolt_id,separateBolt).shuffleGrouping(spout_id);

        //添加配置
        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);

        //本地提交
        localCluster.submitTopology("Gentear",conf ,builder.createTopology());

        //停止
        Time.sleep(100000);
        localCluster.killTopology("Gentear");
        localCluster.shutdown();

    }

}
