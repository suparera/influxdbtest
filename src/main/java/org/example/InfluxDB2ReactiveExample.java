package org.example;

import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.influxdb.client.reactive.QueryReactiveApi;

import io.reactivex.rxjava3.core.Flowable;

public class InfluxDB2ReactiveExample {

    private static char[] token = "MJNUaQG9fIx7U1xJPtRwN2E77s2Ff8YwWAOck4CZacNDR180Y1wNPnUmX90K9sf8OXWSGRT7lozBFQkAzsaymw==".toCharArray();
    private static String org = "org1";

    public static void main(final String[] args) {

        InfluxDBClientReactive influxDBClient = InfluxDBClientReactiveFactory.create("http://localhost:8086", token, org);

        //
        // Query data
        //
        String flux = "from(bucket:\"buck1\") |> range(start: 0)";

        QueryReactiveApi queryApi = influxDBClient.getQueryReactiveApi();

        Flowable.fromPublisher(queryApi.query(flux))
                //
                // Filter records by measurement name
                //
                .filter(it -> "temperature".equals(it.getMeasurement()))
                //
                // Take first 10 records
                //
                //.take(2)
                .subscribe(fluxRecord -> {
                    //
                    // The callback to consume a FluxRecord.
                    //
                    System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
                });

        influxDBClient.close();
    }
}