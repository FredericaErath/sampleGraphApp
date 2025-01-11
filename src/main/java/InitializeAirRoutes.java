import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

/**
 * @version 1.0
 * @description InitializeAirRoutes
 * @date 2025/1/10 16:09
 *
 *
 * Notes:
 * Basic concepts:
 * 1. TinkerPop:  graph computing framework
 * 2. JanusGraph: support the processing of graphs so large that they require storage and computational
 * capacities beyond what a single machine can provide.
 * 3. Gremlin: a JanusGraph’s query language used to retrieve data from and modify data in the graph,
 * a component of Apache TinkerPop
 */

public class InitializeAirRoutes {
    public static void main(String[] args) throws IOException {
        // Create an in-memory JanusGraph
        // InmemoryLoader();
//        try (JanusGraph graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open()) {
//            Loader(graph);
//        }

        //BerkeleyDBLoader();
        // reference: https://docs.janusgraph.org/v1.0/storage-backend/bdb/
        try (JanusGraph graph = JanusGraphFactory.build().
                set("storage.backend", "berkeleyje").
                set("storage.directory", "./data/berkeley").
                open();) {Loader(graph);}
        File storageDir = new File("./data/berkeley");
        long size = Files.walk(storageDir.toPath())
                .filter(p -> p.toFile().isFile())
                .mapToLong(p -> p.toFile().length())
                .sum();
        System.out.println("Storage size: " + size + " bytes");


    }

    public static void Loader(JanusGraph graph){
        // reference https://docs.janusgraph.org/v1.0/storage-backend/inmemorybackend/

        // Initialize schema
        initializeSchema(graph);
        System.out.println("Schema initialized.");

        // Load sample data (to be implemented)
        long startTime = System.currentTimeMillis();
        loadSampleData(graph);
        long endTime = System.currentTimeMillis();
        System.out.println("Sample data loaded, takes: " + (endTime - startTime));
        long vertexCount = graph.traversal().V().count().next();
        System.out.println("Total vertices: " + vertexCount);
        long edgeCount = graph.traversal().E().count().next();
        System.out.println("Total edges: " + edgeCount);
    }


    private static void initializeSchema(JanusGraph graph) {
        // reference: https://docs.janusgraph.org/v1.0/schema/
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // read JSON file
            JsonNode schema = objectMapper.readTree(new File("D:/新建文件夹/researchproj/SampleGraphApp/src/data/schema.json"));
            JanusGraphManagement mgmt = graph.openManagement();

            // create Vertex Labels
            for (JsonNode vertexLabel : schema.get("vertexLabels")) {
                String name = vertexLabel.get("name").asText();
                mgmt.makeVertexLabel(name).make();
            }

            // create Edge Labels
            for (JsonNode edgeLabel : schema.get("edgeLabels")) {
                String name = edgeLabel.get("name").asText();
                // multi: Allows multiple edges of the same label between any pair of vertices.
                Multiplicity multiplicity = Multiplicity.valueOf(edgeLabel.get("multiplicity").asText());
                mgmt.makeEdgeLabel(name).multiplicity(multiplicity).make();
            }

            // create Property Keys
            for (JsonNode propertyKey : schema.get("propertyKeys")) {
                String name = propertyKey.get("name").asText();
                String dataType = propertyKey.get("dataType").asText();
                Cardinality cardinality = Cardinality.valueOf(propertyKey.get("cardinality").asText());
                Class<?> clazz = getClassForDataType(dataType);

                mgmt.makePropertyKey(name).dataType(clazz).cardinality(cardinality).make();
            }

            // indices
            JsonNode graphIndices = schema.get("graphIndices");
            if (graphIndices != null && graphIndices.has("compositeIndices")) {
                for (JsonNode compositeIndex : graphIndices.get("compositeIndices")) {
                    String indexName = compositeIndex.get("indexName").asText();
                    String elementType = compositeIndex.get("elementType").asText();
                    boolean unique = compositeIndex.has("unique") && compositeIndex.get("unique").asBoolean();
                    String indexOnly = compositeIndex.has("indexOnly") ? compositeIndex.get("indexOnly").asText() : null;
                    JsonNode propertyKeys = compositeIndex.get("propertyKeys");

                    if (mgmt.getGraphIndex(indexName) == null) {
                        JanusGraphManagement.IndexBuilder indexBuilder = mgmt.buildIndex(indexName, getElementType(elementType));

                        for (JsonNode key : propertyKeys) {
                            indexBuilder.addKey(mgmt.getPropertyKey(key.asText()));
                        }
                        if (indexOnly != null) {
                            indexBuilder.indexOnly(mgmt.getVertexLabel(indexOnly));
                        }
                        if (unique) {
                            indexBuilder.unique();
                        }
                        indexBuilder.buildCompositeIndex();
                        System.out.println("Created composite index: " + indexName);
                    }
                }
            }
            mgmt.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Class<? extends Element> getElementType(String elementType) {
        // helper function to get element types
        switch (elementType) {
            case "vertex":
                return Vertex.class;
            case "edge":
                return Edge.class;
            default:
                throw new IllegalArgumentException("Unsupported element type: " + elementType);
        }
    }

    private static Class<?> getClassForDataType(String dataType) {
        // helper function to get data types
        switch (dataType) {
            case "String":
                return String.class;
            case "Double":
                return Double.class;
            case "Integer":
                return Integer.class;
            case "Boolean":
                return Boolean.class;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private static Object parseValue(String value, String header) {
        // helper function to parse value according to csv headers
        if (header.contains(":int")) {
            return Integer.parseInt(value);
        } else if (header.contains(":double")) {
            return Double.parseDouble(value);
        } else {
            return value;
        }
    }

    private static void loadSampleData(JanusGraph graph) {
        // load data from CSV files
        try {
            loadNodes(graph);

            loadEdges(graph);

            System.out.println("Data loaded successfully.");
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

    }

    private static void loadNodes(JanusGraph graph) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader("D:/新建文件夹/researchproj/SampleGraphApp/src/data/air-routes-latest-nodes.csv"))) {
            List<String[]> rows = reader.readAll();
            String[] headers = rows.get(0);

            JanusGraphTransaction tx = graph.newTransaction();

            for (int i = 2; i < rows.size(); i++) { // start from third lines
                String[] fields = rows.get(i);

                // create vertex according to  ~label and ~id
                String customId = fields[0].trim();
                String label = fields[1].trim();
                Vertex vertex = tx.addVertex(label);
                vertex.property("identity", customId);
                //set other properties
                for (int j = 2; j < headers.length-2; j++) {
                    String header = headers[j].split(":")[0];
                    String value = fields[j].trim();
                    if (!value.isEmpty()) {
                        vertex.property(header, parseValue(value, headers[j]));
                    }
                }
            }

            tx.commit();
        }
    }

    private static void loadEdges(JanusGraph graph) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader("D:/新建文件夹/researchproj/SampleGraphApp/src/data/air-routes-latest-edges.csv"))) {
            List<String[]> rows = reader.readAll();

            JanusGraphTransaction tx = graph.newTransaction();
            try {
                for (int i = 1; i < rows.size(); i++) {
                    String[] row = rows.get(i);

                    String fromId = row[1];
                    String toId = row[2];
                    String label = row[3];
                    String dist = row[4];

                    // find from and to
                    Vertex fromVertex = tx.traversal().V().has("identity", fromId).next();
                    Vertex toVertex = tx.traversal().V().has("identity", toId).next();

                    // create vertex
                    if (fromVertex != null && toVertex != null) {
                        try{
                            if(!Objects.equals(row[4], "")){
                                // dist can be "" for "contains"
                                fromVertex.addEdge(label, toVertex, "dist", Integer.parseInt(dist));
                            }else {
                                fromVertex.addEdge(label, toVertex);
                            }
                        }catch (Exception e){
                            System.out.println(e);
                        }

                    }
                }
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }}
}
