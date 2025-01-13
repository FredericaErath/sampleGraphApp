import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
//        try (JanusGraph graph = JanusGraphFactory.build().
//                set("storage.backend", "berkeleyje").
//                set("storage.directory", "./data/berkeley").
//                open();) {Loader(graph);}
//        File storageDir = new File("./data/berkeley");
//        long size = Files.walk(storageDir.toPath())
//                .filter(p -> p.toFile().isFile())
//                .mapToLong(p -> p.toFile().length())
//                .sum();
//        System.out.println("Storage size: " + size + " bytes");

        // FDB clear all old fdb data
        FDB fdb = FDB.selectAPIVersion(620); // Choose the correct version
        Database db = fdb.open();
        try (Transaction tr = db.createTransaction()) {
            Range range = new Range(new byte[]{}, new byte[]{(byte) 0xFF});
            tr.clear(range);
            tr.commit().join();
            System.out.println("All key-value pairs have been cleared.");
        }


        try (JanusGraph graph = JanusGraphFactory.build().set("storage.backend", "org.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager").open()) {Loader(graph);}



    }

    public static void Loader(JanusGraph graph){
        // reference https://docs.janusgraph.org/v1.0/storage-backend/inmemorybackend/

        // Initialize schema
        initializeSchema(graph);
        System.out.println("Schema initialized.");

        // Load sample data (to be implemented)
        long startTime = System.currentTimeMillis();
        try {
            loadSampleData(graph);
        } catch (Exception e){
            System.out.println(e);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Sample data loaded, takes: " + (endTime - startTime));
        long vertexCount = graph.traversal().V().count().next();
        System.out.println("Total vertices: " + vertexCount);
        long edgeCount = graph.traversal().E().count().next();
        System.out.println("Total edges: " + edgeCount);
    }


    public static void initializeSchema(JanusGraph graph) {
        // reference: https://docs.janusgraph.org/v1.0/schema/
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // read JSON file
            JsonNode schema = objectMapper.readTree(new File("./src/data/schema.json"));
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

            // index
            if (!mgmt.containsPropertyKey("identity")) {
                mgmt.makePropertyKey("identity").dataType(String.class).make();
            }

            if (!mgmt.containsGraphIndex("identityIndex")) {
                mgmt.buildIndex("identityIndex", Vertex.class)
                        .addKey(mgmt.getPropertyKey("identity"))
                        .unique()
                        .buildCompositeIndex();
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

    public static void loadSampleData(JanusGraph graph) {
        // load data from CSV files
        try {
            loadNodes(graph);

            loadEdges(graph, 100);

            System.out.println("Data loaded successfully.");
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

    }

    public static void loadNodes(JanusGraph graph) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader("./src/data/air-routes-latest-nodes.csv"))) {
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

    private static void loadEdges(JanusGraph graph, int batchSize) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader("./src/data/air-routes-latest-edges.csv"))) {
            List<String[]> rows = reader.readAll();

            List<String[]> batch = new ArrayList<>();
            for (int i = 1; i < rows.size(); i++) {
                batch.add(rows.get(i));

                // use batch to avoid "Transaction is not restartable" error
                if (batch.size() == batchSize || i == rows.size() - 1) {
                    try (JanusGraphTransaction tx = graph.newTransaction()) {
                        Map<String, Vertex> vertexCache = new HashMap<>();
                        for (String[] row : batch) {
                            String fromId = row[1];
                            String toId = row[2];
                            String label = row[3];
                            String dist = row[4];

                            Vertex fromVertex = vertexCache.computeIfAbsent(fromId, id ->
                                    tx.traversal().V().has("identity", id).tryNext().orElse(null)
                            );
                            Vertex toVertex = vertexCache.computeIfAbsent(toId, id ->
                                    tx.traversal().V().has("identity", id).tryNext().orElse(null)
                            );

                            if (fromVertex != null && toVertex != null) {
                                if (!dist.isEmpty()) {
                                    fromVertex.addEdge(label, toVertex, "dist", Integer.parseInt(dist));
                                } else {
                                    fromVertex.addEdge(label, toVertex);
                                }
                            }
                        }
                        tx.commit();
                        batch.clear(); // clear batch
                    } catch (Exception e) {
                        System.err.println("Failed to insert batch of edges: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
