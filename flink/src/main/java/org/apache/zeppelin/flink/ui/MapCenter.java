package org.apache.zeppelin.flink.ui;

import java.io.Serializable;

/**
 * Created by philipp on 7/10/16.
 */
public class MapCenter implements Serializable {
    public double lat = 10.0;
    public double lng = 10.0;;
    public int zoom = 10;
    public boolean autoDiscover = false;

    @Override
    public String toString() {
        return "MapCenter{" +
                "lat=" + lat +
                ", lng=" + lng +
                ", zoom=" + zoom +
                ", autoDiscover=" + autoDiscover +
                '}';
    }
}
