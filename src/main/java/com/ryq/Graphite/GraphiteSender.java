package com.ryq.Graphite;

import com.ryq.Kafka.KafkaData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.python.core.*;
import org.python.modules.cPickle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class GraphiteSender {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public void send(ConsumerRecords<String, String> records) {
        //将kafka中的数据传递到一个list中，并将这个list发送到loalhost:2004
        try (Socket socket = new Socket("114.115.236.121", 9090))  {
            PyList list = new PyList();
            records.forEach(record -> {
                addTestRecord(record, list);
            });
            PyString payload = cPickle.dumps(list);
            byte[] header = ByteBuffer.allocate(4).putInt(payload.__len__()).array();
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(header);
            outputStream.write(payload.toBytes());
            outputStream.flush();
        } catch (IOException e) {
            logger.error("Exception thrown writing data to graphite: " + e);
        }
    }
    //添加测试数据
    private void addTestRecord(ConsumerRecord<String, String> record, PyList list) {
        addFloatMetric(record, list);
    }
    //下面这个是真实场景下的数据方法 可以根据自己的实际情况添加
    private void addWindSpeed(ConsumerRecord<String, String> record, PyList list) {
        addFloatMetric(record, list);
    }

    private void addFloatMetric(ConsumerRecord<String, String> record, List list) {
        KafkaData data;
        String[] temp = record.value().split("\\s+");
        data = new KafkaData(temp[0].split("\\.")[2],temp[1],temp[2]);
        if (data.value == null) {
            // Some values are optional or not giving data due to broken sensors etc
            return;
        }

        //LocalDateTime dateTime = LocalDateTime.parse(data.dataDate);
        //LocalDateTime dateTime =LocalDateTime.ofEpochSecond(,0, ZoneOffset.ofHours(8));
        //graphite需要的数据个是为 name value timestamp
        PyString metricName = new PyString(record.topic() + "." + data.id);
        PyInteger timestamp = new PyInteger(Integer.parseInt(data.dataDate.substring(0,10)));
        PyFloat metricValue = new PyFloat(Double.parseDouble(data.value));
        PyTuple metric = new PyTuple(metricName, new PyTuple(timestamp, metricValue));
        logMetric(metric);
        list.add(metric);
    }

    private void logMetric(PyTuple metric) {
        logger.info("Added metric: " + metric.toString());
    }
}
