package com.dhl.dci.datagrid.data;

import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.stereotype.Component;

import java.util.*;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
@Slf4j
public class GeneratedSchema {

    private static final String INIT_INPUTS = "input DateRange {\n" +
            "\tfrom: String\n" +
            "\tto: String\n" +
            "}\n";

    private static final String INIT_QUERY = "type Query {\n";

    private static final String CLOSE_EXP = "}\n";

    @Autowired
    ElasticsearchRestTemplate elasticsearchTemplate;

    @Autowired
    RestHighLevelClient elasticSearchClient;

    private Set<EsIndex> indexes;

    public void initIndexes(List<String> indexNames) {
        indexes = new HashSet<>();
        for (String index : indexNames) {
            log.debug("Filling index: " + index);
            Map<String, Object> esSchema = elasticsearchTemplate.indexOps(IndexCoordinates.of(index)).getMapping();
            Map properties = (Map) esSchema.get("properties");
            Map<String, String> indexProperties = new HashMap<>();
            properties.keySet().forEach(key -> {
                String type = (String) ((Map) properties.get(key)).get("type");
                if (type != null) {
                    indexProperties.put((String) key, type);
                }
            });
            indexes.add(new EsIndex(index, indexProperties));
        }
    }

    @Override
    public String toString() {
        StringBuilder inputs = new StringBuilder(INIT_INPUTS);
        StringBuilder queries = new StringBuilder(INIT_QUERY);
        StringBuilder results = new StringBuilder();
        for (EsIndex index : indexes) {
            inputs.append("input " + index.getFilterName() + "{\n");
            queries.append("\t" + index.getQueryName() + "(filter:" + index.getFilterName() + "): [" + index.getResultName() + "]!\n");
            results.append("type " + index.getResultName() + " {\n");
            Map<String, String> properties = index.getProperties();
            properties.keySet().forEach(key -> {
                String type = properties.get(key);
                if (type != null) {
                    switch (type) {
                        case "date":
                            inputs.append("\t" + key + ": DateRange\n");
                            results.append("\t" + key + ": String\n");
                            break;
                        case "keyword":
                            inputs.append("\t" + key + ": String\n");
                            results.append("\t" + key + ": String\n");
                            break;
                        case "integer":
                            inputs.append("\t" + key + ": Int\n");
                            results.append("\t" + key + ": Int\n");
                            break;
                        case "scaled_float":
                            inputs.append("\t" + key + ": Float\n");
                            results.append("\t" + key + ": Float\n");
                            break;
                        case "boolean":
                            inputs.append("\t" + key + ": Boolean\n");
                            results.append("\t" + key + ": Boolean\n");
                            break;
                        default:
                    }
                }
            });
            inputs.append(CLOSE_EXP);
            results.append(CLOSE_EXP);
        }
        queries.append(CLOSE_EXP);
        return inputs.append(results).append(queries).toString();
    }

    public RuntimeWiring buildWiring() {
        TypeRuntimeWiring.Builder builder = newTypeWiring("Query");
        indexes.forEach(index -> {
            builder.dataFetcher(index.getQueryName(), getList(index.getName(), index.getProperties()));
        });
        return RuntimeWiring.newRuntimeWiring().type(builder).build();
    }

    private DataFetcher getList(String index, Map<String, String> properties) {
        return dataFetchingEnvironment -> {
            SearchRequest request = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            request.source(sourceBuilder);
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            sourceBuilder.query(queryBuilder);
            Map filter = (Map) dataFetchingEnvironment.getArguments().get("filter");
            filter.keySet().forEach(key -> {
                String sKey = (String) key;
                String type = properties.get(sKey);
                if (type == null) {
                    log.warn("Type " + sKey + " is not supported in request");
                } else {
                    switch (type) {
                        case "keyword":
                            queryBuilder.filter(new TermQueryBuilder(sKey, filter.get(key)));
                            break;
                        case "date":
                            Map dateRange = (Map) filter.get(key);
                            queryBuilder.filter(getRangeQuery(sKey, (String) dateRange.get("from"), (String) dateRange.get("to")));
                            break;
                        default:
                            queryBuilder.filter(new MatchQueryBuilder(sKey, filter.get(key)));
                    }
                }
            });
            log.info("Request:" + request);
            SearchHits hits = elasticSearchClient.search(request, RequestOptions.DEFAULT).getHits();
            List result = new ArrayList();
            hits.forEach(hit -> result.add(hit.getSourceAsMap()));
            return result;
        };
    }

    public static RangeQueryBuilder getRangeQuery(String field, String from, String to) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
        if (from != null) {
            rangeQueryBuilder.gte(from);
        }
        if (to != null) {
            rangeQueryBuilder.lte(to);
        }
        return rangeQueryBuilder;
    }
}
