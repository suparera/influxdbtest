package org.example;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.influxdb.client.reactive.WriteReactiveApi;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.reactivestreams.Publisher;

public class InfluxDB2ReactiveExampleWriteEveryTenSeconds {

    private static char[] token = "MJNUaQG9fIx7U1xJPtRwN2E77s2Ff8YwWAOck4CZacNDR180Y1wNPnUmX90K9sf8OXWSGRT7lozBFQkAzsaymw==".toCharArray();
    private static String org = "org1";
    private static String bucket = "buck1";

    public static void main(final String[] args) throws InterruptedException {

        InfluxDBClientReactive influxDBClient = InfluxDBClientReactiveFactory.create("http://localhost:8086", token, org, bucket);

        //
        // Write data
        //
        WriteReactiveApi writeApi = influxDBClient.getWriteReactiveApi();

        Flowable<Temperature> measurements = Flowable.interval(1, TimeUnit.SECONDS)
                .map(time -> {

                    Temperature temperature = new Temperature();
                    temperature.location = getLocation();
                    temperature.value = getValue();
                    temperature.time = Instant.now();
                    return temperature;
                });

        //
        // ReactiveStreams publisher
        //
        Publisher<WriteReactiveApi.Success> publisher = writeApi.writeMeasurements(WritePrecision.NS, measurements);

        //
        // Subscribe to Publisher
        //
        Disposable subscriber = Flowable.fromPublisher(publisher)
                .subscribe(success -> System.out.println("Successfully written temperature"));

        Thread.sleep(35_000);

        subscriber.dispose();

        influxDBClient.close();
    }

    private static Double getValue() {
        Random r = new Random();

        return -20 + 70 * r.nextDouble();
    }

    private static String getLocation() {
        return "Prague";
    }

    @Measurement(name = "temperature")
    private static class Temperature {

        @Column(tag = true)
        String location;

        @Column
        Double value;

        @Column(timestamp = true)
        Instant time;
    }
}