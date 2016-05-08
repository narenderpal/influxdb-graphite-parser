package org.influxdb.graphite;


import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Point {
    private String measurement;
    private Map<String, String> tags;
    private Long time;
    private TimeUnit precision;
    private Map<String, Object> fields;

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public TimeUnit getPrecision() {
        return precision;
    }

    public void setPrecision(TimeUnit precision) {
        this.precision = precision;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "Point{" +
                "measurement='" + measurement + '\'' +
                ", tags=" + tags +
                ", time=" + time +
                ", precision=" + precision +
                ", fields=" + fields +
                '}';
    }
}
