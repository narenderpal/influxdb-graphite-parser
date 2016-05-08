package org.influxdb.graphite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class ParserTest {

    private static Logger LOG = LoggerFactory.getLogger(ParserTest.class);
    private static final boolean IS_ENABLED = true;
    private Parser graphiteMetricParser = new Parser("/parser_templates.json");

    @Test(enabled = IS_ENABLED)
    public void TestParse() {

        String state_metric = "testmetrics.2c3c9f9e-73d9-4460-a668-047162ff1bac.Site1.host-249.metrics_manager.linus_state.serviceState 0 1459513562\n";
        String media_count_metric = "testmetrics.2c3c9f9e-73d9-4460-a668-047162ff1bac.Bangalore-Site2.sx-controller-28_xyz_com.metrics_manager.linus_media.activeMediaCount 4 1459513317\n";
        String cpu_overload_metric = "statsd.2c3c9f9e-73d9-4460-a668-047162ff1bac.Bangalore-Site3.host-247_xyz_com.media_agent.cpu_overload 1 1459845230\n";
        String cpu_usage_metric = "testmetrics.2c3c9f9e-73d9-4460-a668-047162ff1bac.Bangalore-Site4.host-246_xyz_com.linus.cpu_usage.percent 10.3839416610000000 1459513562\n";
        String memory_stats_metric = "testmetrics.2c3c9f9e-73d9-4460-a668-047162ff1bac.Bangalore-Site5.host-246_xyz_com.linus.memory_stats.rss 10.3839416610000000 1459513562\n";

        try {
            Point p1 = graphiteMetricParser.parse(state_metric);
            Point p2 = graphiteMetricParser.parse(media_count_metric);
            Point p3 = graphiteMetricParser.parse(cpu_overload_metric);
            Point p4 = graphiteMetricParser.parse(cpu_usage_metric);
            Point p5 = graphiteMetricParser.parse(memory_stats_metric);

            assertThat(p1.getMeasurement()).isEqualTo("linus_state");
            assertThat(p1.getTags()).containsOnlyKeys("cluster_name", "application_name", "organization_id", "host_name");
            assertThat(p1.getTags()).containsValues("Site1", "metrics_manager", "2c3c9f9e-73d9-4460-a668-047162ff1bac", "host-249");
            assertThat(p1.getFields()).containsKey("serviceState");
            assertThat(p1.getFields()).containsValue(0.0f);

            assertThat(p2.getMeasurement()).isEqualTo("linus_media");
            assertThat(p2.getTags()).containsOnlyKeys("cluster_name", "application_name", "organization_id", "host_name");
            assertThat(p2.getTags()).containsValues("Bangalore-Site2", "metrics_manager", "2c3c9f9e-73d9-4460-a668-047162ff1bac", "sx-controller-28_xyz_com");
            assertThat(p2.getFields()).containsKey("activeMediaCount");
            assertThat(p2.getFields()).containsValue(4.0f);

            assertThat(p3.getMeasurement()).isEqualTo("media_agent");
            assertThat(p3.getTags()).containsOnlyKeys("cluster_name", "organization_id", "host_name");
            assertThat(p3.getTags()).containsValues("Bangalore-Site3", "2c3c9f9e-73d9-4460-a668-047162ff1bac", "host-247_xyz_com");
            assertThat(p3.getFields()).containsKey("cpu_overload");
            assertThat(p3.getFields()).containsValue(1.0f);

            assertThat(p4.getMeasurement()).isEqualTo("cpu_usage");
            assertThat(p4.getTags()).containsOnlyKeys("cluster_name", "application_name", "organization_id", "host_name");
            assertThat(p4.getTags()).containsValues("Bangalore-Site4", "linus", "2c3c9f9e-73d9-4460-a668-047162ff1bac", "host-246_xyz_com");
            assertThat(p4.getFields()).containsKey("percent");
            assertThat(p4.getFields()).containsValue(10.383942f);

            assertThat(p5.getMeasurement()).isEqualTo("memory_stats");
            assertThat(p5.getTags()).containsOnlyKeys("cluster_name", "application_name", "organization_id", "host_name");
            assertThat(p5.getTags()).containsValues("Bangalore-Site5", "linus", "2c3c9f9e-73d9-4460-a668-047162ff1bac", "host-246_xyz_com");
            assertThat(p5.getFields()).containsKey("rss");
            assertThat(p5.getFields()).containsValue(10.383942f);

        } catch (Exception e) {
            Assert.fail();
        }
    }

    // missing metric field
    @Test(enabled = IS_ENABLED)
    public void TestMissingMetricFieldError() {
        String missing_fields = "1419972457825";
        try {
            graphiteMetricParser.parse(missing_fields);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    //should error parsing invalid float
    @Test(enabled = IS_ENABLED)
    public void TestParseInvalidFloat() {
        String invalid_float = "cpu 50.554z 1419972457825";
        try {
            graphiteMetricParser.parse(invalid_float);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    //should error parsing invalid int
    @Test(enabled = IS_ENABLED)
    public void TestParseInvalidInt() {
        String invalid_int = "cpu 50z 1419972457825";
        try {
            graphiteMetricParser.parse(invalid_int);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    //should error parsing invalid time
    @Test(enabled = IS_ENABLED)
    public void TestParseInvalidTime() {
        String invalid_time = "cpu 50.554 14199724z57825";
        try {
            graphiteMetricParser.parse(invalid_time);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    // default template
    @Test(enabled = IS_ENABLED)
    public void TestFilterMatchDefault() {
        String match_default_template = "cpu 50.55 141997247825";
        try {
            Point p1 = graphiteMetricParser.parse(match_default_template);
            assertThat(p1.getMeasurement()).isEqualTo("cpu");
            assertThat(p1.getTags()).isEmpty();
            assertThat(p1.getFields()).containsKey("value");
            assertThat(p1.getFields()).containsValue(50.55f);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test(enabled = false)
    public void TestParseMissingMeasurement() {
        // add new template with missing measurement
    }

    @Test(enabled = false)
    public void TestFilterMatchMultipleMeasurement() {
        // add new template with multiple measurement fields
    }

    @Test(enabled = false)
    public void TestParseNoMatch() {
        // this is a default template match case
    }

    @Test(enabled = false)
    public void TestFilterMatchMostLongestFilter() {
        // add new two template with matching regex with different length
    }

    @Test(enabled = false)
    public void TestParseDefaultTemplateTags() {
        // only per template tags are supported, add new template with default tags
    }

    @Test(enabled = false)
    public void TestParseDefaultTemplateTagsOverridGlobal() {
        // only per template tags are supported
    }

    @Test(enabled = false)
    public void TestParseDefaultTags() {
        // only per template tags are supported
    }

}
