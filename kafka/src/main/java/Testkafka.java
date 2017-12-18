import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Testkafka {
    public static void main(String[] arg){
        //按照kafka官网启动服务后，要注意修改kafka配置文件中的zookeeper.properties中的dataDir的路径存在
        //然后启动代码目录下静态文件中的kafka服务
        //1. bin/zookeeper-server-start.sh config/zookeeper.properties
        //2. bin/kafka-server-start.sh config/server.properties
        //完成上面步骤后，就可以运行本段代码
        //创建生产者start
        Map<String, String> config_product = new HashMap<>();
        //kafka默认的服务端口
        config_product.put("bootstrap.servers", "localhost:9092");
        config_product.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config_product.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config_product.put("acks", "1");
        Vertx vertx = Vertx.vertx();
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config_product);
        //创建消费者end

        Map<String, String> config_consume = new HashMap<>();
        config_consume.put("bootstrap.servers", "localhost:9092");
        config_consume.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config_consume.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config_consume.put("group.id", "my_group");
        config_consume.put("auto.offset.reset", "earliest");
        config_consume.put("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config_consume);
        //创建消费者end
        //消费者获取信息，并且处理信息内容
        consumer.handler(record -> {
            System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
        });

// subscribe to several topics
//        Set<String> topics = new HashSet<>();
//        topics.add("topic1");
//        topics.add("topic2");
//        topics.add("topic3");
//        consumer.subscribe(topics);

        //增加主题
        consumer.subscribe("test");
        for (int i = 0; i < 5; i++) {
            // only topic and message value are specified, round robin on destination partitions
            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create("test", "message_" + i);
            producer.write(record);
        }
        //本代码测试的时候应注意多次运行代码后，每次都会比上一次多打印5条信息，这是因为kafka默认保存两个星期的消息，所以每次重新运行的时候，生产着会新增五条信息，但是消费者消费的确实队列中全部的信息
    }
}
