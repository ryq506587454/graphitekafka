package com.ryq.Kafka;

import java.io.Serializable;

public class KafkaData implements Serializable {
    private static final long serialVersionUID = 123L;

    public KafkaData(String id, String value, String dataDate) {
        this.id = id;
        this.value = value;
        this.dataDate = dataDate;
    }

    public String id;
    public String value;
    public String dataDate;
}
