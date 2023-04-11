package org.example;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.events.WriteSuccessEvent;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WriteBatchExample {
    private static String org = "org1";
    private static String bucket = "buck1";
    
    public WriteBatchExample() {
    }
    
    public static void main(String[] args) throws InterruptedException {
        char[] token = args[0].toCharArray();
        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
    
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (WriteApi writeApi = influxDBClient.makeWriteApi(WriteOptions.builder()
                .batchSize(5000)
                .flushInterval(1000)
                .backpressureStrategy(BackpressureOverflowStrategy.DROP_OLDEST)
                .bufferLimit(10000)
                .jitterInterval(1000)
                .retryInterval(5000)
                .build())) {
    
            writeApi.listenEvents(WriteSuccessEvent.class, (value) -> countDownLatch.countDown());
            
            for(int i=0 ; i<10000 ; i++) {
    
                //
                // Write by POJO
                //
                Temperature temperature = new Temperature();
                temperature.location = "south";
                temperature.value = 62D;
                temperature.time = Instant.now();
                writeApi.writeMeasurement("buck1", "org1", WritePrecision.MS, temperature);
            }
    
            countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        }
    }
    
    @Measurement(name = "temperature")
    private static class Temperature {
        public Temperature() {
        }
    
        public Temperature(String location, Double value, Instant time) {
            this.location = location;
            this.value = value;
            this.time = time;
        }
    
        @Column(tag = true)
        String location;
        
        @Column
        Double value;
        
        @Column(timestamp = true)
        Instant time;
    }
}


