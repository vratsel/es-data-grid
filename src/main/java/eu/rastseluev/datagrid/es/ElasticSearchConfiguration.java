package eu.rastseluev.datagrid.es;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.List;

@Configuration
public class ElasticSearchConfiguration extends ElasticsearchConfiguration {

    @Value("${datagrid.elasticsearch.xpack:false}")
    private boolean xpackEnabled;

    @Value("${datagrid.elasticsearch.username:}")
    private String username;

    @Value("${datagrid.elasticsearch.password:}")
    private String password;

    @Value("#{'${datagrid.elasticsearch.cluster-nodes}'.split(',')}")
    private List<String> clusterNodes;

    @Value("${datagrid.elasticsearch.connectionTimeout:1000}")
    private long connectionTimeoutMs;

    @Value("${datagrid.elasticsearch.socketTimeout:30000}")
    private long socketTimeoutMs;

    @Override
    public ClientConfiguration clientConfiguration() {
        String[] endpoints = clusterNodes.stream()
                .map(String::trim)
                .filter(StringUtils::hasText)
                .toArray(String[]::new);

        ClientConfiguration.MaybeSecureClientConfigurationBuilder builder = (ClientConfiguration.MaybeSecureClientConfigurationBuilder)ClientConfiguration.builder()
                .connectedTo(endpoints)
                .withConnectTimeout(Duration.ofMillis(connectionTimeoutMs))
                .withSocketTimeout(Duration.ofMillis(socketTimeoutMs));

        if (xpackEnabled) {
            builder = (ClientConfiguration.MaybeSecureClientConfigurationBuilder)builder.usingSsl();
        }

        if (StringUtils.hasText(username) && StringUtils.hasText(password)) {
            builder = (ClientConfiguration.MaybeSecureClientConfigurationBuilder)builder.withBasicAuth(username, password);
        }

        return builder.build();
    }
}