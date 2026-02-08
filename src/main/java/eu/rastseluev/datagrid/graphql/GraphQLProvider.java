package eu.rastseluev.datagrid.graphql;

import eu.rastseluev.datagrid.data.GeneratedSchema;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.graphql.execution.GraphQlSource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class GraphQLProvider {

    @Autowired
    GeneratedSchema schema;

    @Value("#{'${datagrid.elasticsearch.indexes}'.split(',')}")
    private List<String> indexes;

    @Bean
    public GraphQlSource graphQlSource() {
        schema.initIndexes(indexes);

        String sdlCreated = schema.toString();
        log.info("Generated SDL:\n{}", sdlCreated);

        GraphQLSchema graphQLSchema = buildSchema(sdlCreated);

        // Spring GraphQL will use this to serve POST {spring.graphql.path:/graphql}
        return GraphQlSource.builder(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, schema.buildWiring());
    }
}