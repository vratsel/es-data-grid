package eu.rastseluev.datagrid.es;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.ElasticsearchConfigurationSupport;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.util.StringUtils;

import java.util.List;

@Configuration
public class ElasticSearchConfiguration extends ElasticsearchConfigurationSupport {

    @Value("${datagrid.elasticsearch.xpack:false}")
    private boolean xpackEnabled;

    @Value("${datagrid.elasticsearch.username:}")
    private String username;

    @Value("${datagrid.elasticsearch.password:}")
    private String password;

    @Value("#{'${datagrid.elasticsearch.cluster-nodes}'.split(',')}")
    private List<String> clusterNodes;

    @Value("${datagrid.elasticsearch.connectionTimeout}")
    private int connectionTimeout;

    @Value("${datagrid.elasticsearch.socketTimeout}")
    private int socketTimeout;

    @Bean
    public RestHighLevelClient elasticsearchClient() {

        HttpHost[] hosts = new HttpHost[clusterNodes.size()];
        for (int i = 0; i < clusterNodes.size(); i++) {
            //split to hostname and port
            String[] split = clusterNodes.get(i).split(":");
            hosts[i] = new HttpHost(split[0], Integer.parseInt(split[1]), xpackEnabled ? "https" : "http");
        }
        RestClientBuilder lowLevelClientBuilder = RestClient.builder(hosts)
                .setRequestConfigCallback(builder -> builder
                        .setConnectTimeout(connectionTimeout)
                        .setSocketTimeout(socketTimeout)
                        .setConnectionRequestTimeout(0)
                );

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            lowLevelClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return new RestHighLevelClient(lowLevelClientBuilder);
    }

    @Bean(name = {"elasticsearchOperations", "elasticsearchTemplate"})
    public ElasticsearchRestTemplate elasticsearchOperations() {
        return new ElasticsearchRestTemplate(elasticsearchClient());
    }
}
