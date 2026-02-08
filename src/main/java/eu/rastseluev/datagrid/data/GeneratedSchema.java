package eu.rastseluev.datagrid.data;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import graphql.schema.DataFetcher;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.stereotype.Component;

import java.util.*;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

@Component
@Slf4j
@RequiredArgsConstructor
public class GeneratedSchema {

    private static final String INIT_INPUTS = """
            input DateRange {
            \tfrom: String
            \tto: String
            }
            """;
    private static final String INIT_QUERY = "type Query {\n";
    private static final String CLOSE_EXP = "}\n";

    private static final String TYPE_QUERY = "Query";

    private static final String ARG_FILTER = "filter";
    private static final String ES_MAPPING_PROPERTIES = "properties";
    private static final String ES_FIELD_TYPE = "type";

    private static final String RANGE_FROM = "from";
    private static final String RANGE_TO = "to";

    private static final String ES_TYPE_DATE = "date";
    private static final String ES_TYPE_KEYWORD = "keyword";
    private static final String ES_TYPE_TEXT = "text";
    private static final String ES_TYPE_INTEGER = "integer";
    private static final String ES_TYPE_LONG = "long";
    private static final String ES_TYPE_SCALED_FLOAT = "scaled_float";
    private static final String ES_TYPE_BOOLEAN = "boolean";

    private final ElasticsearchOperations elasticsearch;

    private Set<EsIndex> indexes = Collections.emptySet();

    public void initIndexes(List<String> indexNames) {
        Objects.requireNonNull(indexNames, "indexNames must not be null");

        Set<EsIndex> newIndexes = new HashSet<>();
        for (String indexName : indexNames) {
            log.debug("Filling index: {}", indexName);
            newIndexes.add(new EsIndex(indexName, readIndexProperties(indexName)));
        }
        this.indexes = newIndexes;
    }

    private Map<String, String> readIndexProperties(String indexName) {
        Map<String, Object> mapping = elasticsearch.indexOps(IndexCoordinates.of(indexName)).getMapping();
        Object rawProperties = mapping.get(ES_MAPPING_PROPERTIES);
        if (!(rawProperties instanceof Map<?, ?> properties)) {
            return Collections.emptyMap();
        }

        Map<String, String> indexProperties = new HashMap<>();
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (!(entry.getKey() instanceof String fieldName)) {
                continue;
            }
            if (!(entry.getValue() instanceof Map<?, ?> fieldDef)) {
                continue;
            }
            Object rawType = fieldDef.get(ES_FIELD_TYPE);
            if (rawType instanceof String type) {
                indexProperties.put(fieldName, type);
            }
        }
        return indexProperties;
    }

    @Override
    public String toString() {
        StringBuilder inputs = new StringBuilder(INIT_INPUTS);
        StringBuilder queries = new StringBuilder(INIT_QUERY);
        StringBuilder results = new StringBuilder();

        for (EsIndex index : indexes) {
            appendIndexSchema(inputs, queries, results, index);
        }

        queries.append(CLOSE_EXP);
        return inputs.append(results).append(queries).toString();
    }

    private static void appendIndexSchema(StringBuilder inputs, StringBuilder queries, StringBuilder results, EsIndex index) {
        inputs.append("input ").append(index.getFilterName()).append("{\n");
        queries.append("\t")
                .append(index.getQueryName())
                .append("(filter:")
                .append(index.getFilterName())
                .append("): [")
                .append(index.getResultName())
                .append("]!\n");

        results.append("type ").append(index.getResultName()).append(" {\n");
        appendFieldDefinitions(inputs, results, index.getProperties());
        inputs.append(CLOSE_EXP);
        results.append(CLOSE_EXP);
    }

    private static void appendFieldDefinitions(StringBuilder inputs, StringBuilder results, Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            String esType = entry.getValue();
            if (esType == null) continue;

            switch (esType) {
                case ES_TYPE_DATE -> {
                    inputs.append("\t").append(fieldName).append(": DateRange\n");
                    results.append("\t").append(fieldName).append(": String\n");
                }
                case ES_TYPE_KEYWORD, ES_TYPE_TEXT -> {
                    inputs.append("\t").append(fieldName).append(": String\n");
                    results.append("\t").append(fieldName).append(": String\n");
                }
                case ES_TYPE_INTEGER, ES_TYPE_LONG -> {
                    inputs.append("\t").append(fieldName).append(": Int\n");
                    results.append("\t").append(fieldName).append(": Int\n");
                }
                case ES_TYPE_SCALED_FLOAT -> {
                    inputs.append("\t").append(fieldName).append(": Float\n");
                    results.append("\t").append(fieldName).append(": Float\n");
                }
                case ES_TYPE_BOOLEAN -> {
                    inputs.append("\t").append(fieldName).append(": Boolean\n");
                    results.append("\t").append(fieldName).append(": Boolean\n");
                }
                default -> {
                    // unsupported type: intentionally omitted from schema
                }
            }
        }
    }

    public RuntimeWiring buildWiring() {
        TypeRuntimeWiring.Builder builder = newTypeWiring(TYPE_QUERY);
        indexes.forEach(index ->
                builder.dataFetcher(index.getQueryName(), buildSearchFetcher(index.getName(), index.getProperties()))
        );
        return RuntimeWiring.newRuntimeWiring().type(builder).build();
    }

    private DataFetcher<List<Map<String, Object>>> buildSearchFetcher(String indexName, Map<String, String> properties) {
        return env -> {
            Object rawFilter = env.getArguments().get(ARG_FILTER);
            NativeQuery query = buildNativeQuery(rawFilter, properties);

            var hits = elasticsearch.search(query, Map.class, IndexCoordinates.of(indexName));
            List<Map<String, Object>> result = new ArrayList<>(Math.toIntExact(hits.getTotalHits()));
            for (var hit : hits) {
                var source = hit.getContent();
                //noinspection unchecked
                result.add((Map<String, Object>) source);
            }
            return result;
        };
    }

    private NativeQuery buildNativeQuery(Object rawFilter, Map<String, String> properties) {
        List<Query> filters = new ArrayList<>();
        if (rawFilter instanceof Map<?, ?> filter) {
            applyFilters(filters, filter, properties);
        }

        return NativeQuery.builder()
                .withQuery(q -> q.bool(b -> b.filter(filters)))
                .build();
    }

    private void applyFilters(List<Query> filters, Map<?, ?> filter, Map<String, String> properties) {
        for (Map.Entry<?, ?> entry : filter.entrySet()) {
            if (!(entry.getKey() instanceof String fieldName)) {
                continue;
            }
            String esType = properties.get(fieldName);
            if (esType == null) {
                log.warn("Field '{}' is not supported in request (unknown type)", fieldName);
                continue;
            }
            filters.add(buildFilterQuery(fieldName, esType, entry.getValue()));
        }
    }

    private static Query buildFilterQuery(String fieldName, String esType, Object rawValue) {
        return switch (esType) {
            case ES_TYPE_KEYWORD ->
                    Query.of(q -> q.term(t -> t.field(fieldName).value(v -> v.stringValue(String.valueOf(rawValue)))));
            case ES_TYPE_DATE -> {
                if (rawValue instanceof Map<?, ?> dateRange) {
                    String from = (String) dateRange.get(RANGE_FROM);
                    String to = (String) dateRange.get(RANGE_TO);
                    yield buildRangeQuery(fieldName, from, to);
                }
                yield Query.of(q -> q.match(m -> m.field(fieldName).query(String.valueOf(rawValue))));
            }
            default -> Query.of(q -> q.match(m -> m.field(fieldName).query(String.valueOf(rawValue))));
        };
    }

    private static Query buildRangeQuery(String fieldName, String fromInclusive, String toInclusive) {
        Objects.requireNonNull(fieldName, "fieldName must not be null");

        if (fromInclusive == null && toInclusive == null) {
            return Query.of(q -> q.matchAll(m -> m));
        }
        return Query.of(q -> q.range(rq -> rq.date(r -> {
            r.field(fieldName);
            if (fromInclusive != null) {
                r.gte(fromInclusive);
            }
            if (toInclusive != null) {
                r.lte(toInclusive);
            }
            return r;
        })));
    }
}