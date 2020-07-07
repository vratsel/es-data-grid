package eu.rastseluev.datagrid.graphql;

import eu.rastseluev.datagrid.data.GeneratedSchema;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
@Slf4j
public class GraphQLProvider {

    private GraphQL graphQL;

    @Autowired
    GeneratedSchema schema;

    @Value("#{'${datagrid.elasticsearch.indexes}'.split(',')}")
    private List<String> indexes;

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

    @PostConstruct
    public void init() throws IOException {
        schema.initIndexes(indexes);
        String sdlCreated = schema.toString();
        log.info("Generated SDL:\n" + sdlCreated);
        GraphQLSchema graphQLSchema = buildSchema(sdlCreated);
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, schema.buildWiring());
    }
}
