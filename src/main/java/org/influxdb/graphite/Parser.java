package org.influxdb.graphite;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 * https://github.com/influxdata/influxdb/tree/master/services/graphite
 *
 */
public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);
    private final String default_template = "measurement*";

    private Map <String, String> sorted_template_map = new TreeMap<String, String>(
            new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    if(s1.length() > s2.length()) {
                        return -1;
                    } else if(s1.length() < s2.length()) {
                        return 1;
                    } else {
                        return s1.compareTo(s2);
                    }
                }
            }
    );

    public Parser(String resource) {
        init(resource);
        validateTemplate(this.sorted_template_map);
    }

    private void init(String resource) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String,String> > typeRef = new TypeReference<HashMap<String, String>>() {};

            this.sorted_template_map = mapper.readValue(getClass().getResourceAsStream(resource), typeRef);
            LOG.info("regex template map:" + this.sorted_template_map.toString());
        } catch (IOException e) {
            LOG.error("Failed to parse template file", e);
        }
    }

    public Point parse(String metric) throws Exception {
        // Break metric into three fields (name, value, timestamp)
        String[] fields = metric.split("[\\s\n]+");
        int len = fields.length;
        if (len != 2 && len != 3) {
            LOG.error("Received metric doesn't have required fields:", metric);
            throw new Exception(String.format("Received {%s} which doesn't have required fields", metric));
        }

        // Find matching template for metric
        String template = matchTemplate(fields[0]);

        // Parse out the default tags specific to this template
        Map<String, String> tags = new HashMap<String, String>();
        tags = parseDefaultTags(template.trim(), tags);

        // Template without default tags
        if(!tags.isEmpty()) {
            template = (template.split("[\\s]"))[0];
        }

        // Decode the name and tags
        int tagIndex = -1;
        String fieldName = "";
        List<String> measurementList = new ArrayList<String>();
        String metricNameFields[] = fields[0].split("[.]");
        String templateFields[] = template.split("[.]");
        int numOfMetricNameFields = metricNameFields.length;

        for (String tag: templateFields) {
            ++tagIndex;
            if (tagIndex >= numOfMetricNameFields) {
                continue;
            }

            if (tag.equals("measurement")) {
                measurementList.add(metricNameFields[tagIndex]);
            } else if (tag.equals("field")) {
                if (fieldName.length() != 0) {
                    LOG.error("'field' can only be used once in each template");
                    throw new Exception("field' can only be used once in each template");
                }
                fieldName = metricNameFields[tagIndex];
            } else if (tag.equals("field*")) {
                Joiner joiner = Joiner.on("_");
                fieldName = joiner.join(Arrays.copyOfRange(metricNameFields, tagIndex, numOfMetricNameFields-1));
                break;
            } else if (tag.equals("measurement*")) {
                measurementList.addAll(Arrays.asList(Arrays.copyOfRange(metricNameFields, tagIndex, numOfMetricNameFields-1)));
                break;
            } else if (!tag.isEmpty()) {
                tags.put(tag, metricNameFields[tagIndex]);
            }
        }
        Joiner joiner = Joiner.on(".");
        String measurement = joiner.join(measurementList);
        // Could not extract measurement, use the raw metric name value
        if (measurement.isEmpty()) {
            measurement = fields[0];
        }

        // Parse the field value
        float fieldValue;
        try {
            fieldValue = Float.parseFloat(fields[1]);
        } catch (Exception e) {
            LOG.error(String.format("field %s value: %s",fields[0], e));
            throw new Exception(String.format("field %s value: %s",fields[0], e));
        }

        Map<String, Object> fieldValues = new HashMap<String, Object>();

        if (!fieldName.isEmpty()) {
            fieldValues.put(fieldName, fieldValue);
        } else {
            fieldValues.put("value", fieldValue);
        }

        // If no 3rd field, use now as timestamp
        float timestamp = System.currentTimeMillis();
        if (len == 3) {
            // Parse timestamp
            try {
                timestamp = Float.parseFloat(fields[2]);
            } catch (Exception e) {
                LOG.error(String.format("field %s value: %s",fields[0], e));
                throw new Exception(String.format("field %s value: %s",fields[0], e));
            }
        }

        // Create a new Point with the fields required for influx db write api
        Point point = new Point();
        point.setMeasurement(measurement);
        point.setFields(fieldValues);
        point.setTags(tags);
        point.setTime((long)timestamp);
        point.setPrecision(TimeUnit.MILLISECONDS);
        return point;
    }

    private String matchTemplate(String metric) {
        Set<String> keys = this.sorted_template_map.keySet();
        Iterator<String> itr = keys.iterator();

        while (itr.hasNext()) {
            String regex = itr.next().trim();
            Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(metric);
            if(m.matches()) {
                return this.sorted_template_map.get(regex).trim();
            }
        }
        return default_template;
    }

    private Map<String, String> parseDefaultTags(String template, Map tags) {
        String parts[] = template.split("[\\s]");
        if(parts[parts.length-1].contains("=")) {
            String tagStr[] = parts[parts.length-1].split("[,]");
            for (String kv : tagStr) {
                String tag[] = kv.split("[=]");
                tags.put(tag[0], tag[1]);
            }
        }
        return tags;
    }

    private void validateTemplate(Map templateMap) {

        Set<String> keys = templateMap.keySet();
        Iterator<String> itr = keys.iterator();
        while (itr.hasNext()) {
            String template = (String)templateMap.get(itr.next());
            String templateFields[] = template.trim().split("[ .]");
            boolean hasMeasurement = false;
            boolean hasMeasurementWildcard = false;
            boolean hasFieldWildcard = false;
            for (String tag : templateFields) {
                if (tag.equals("measurement")) {
                    hasMeasurement = true;
                } else if (tag.equals("measurement*")) {
                    hasMeasurementWildcard = true;
                } else if (tag.equals("field*")) {
                    hasFieldWildcard = true;
                }
            }

            if (hasFieldWildcard && hasMeasurementWildcard) {
                LOG.error("Either 'field*' or 'measurement*' can be used in each template but not both together:", template);
                //throw new Exception(String.format("Either 'field*' or 'measurement*' can be used in each template {} but not both together", template));
            }

            if (!hasMeasurement && !hasMeasurementWildcard) {
                LOG.error("No measurement specified for template:", template);
                //throw new Exception(String.format("No measurement specified for template:", template));
            }
        }
    }

}
