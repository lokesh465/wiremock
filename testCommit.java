package com.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Refactored ContractProcessingServiceImpl that implements all the specified requirements.
 */
@Service
@Lazy
public class ContractProcessingServiceImpl implements ContractEventProcessingService {

    private static final Logger LOGGER = LogManager.getLogger(ContractProcessingServiceImpl.class);

    // --- Configuration Properties ---
    @Value("${mongo.collection}")
    private String mongoCollection;

    @Value("${mongo.timetravel.collection}")
    private String mongoCollectionTimetravel;

    @Value("${mongo.field}")
    private String mongoField;

    @Value("${optimized.payload.attributes}")
    private String[] attributes;

    @Value("${optimized.payload.attributes.array}")
    private String[] attributesArray;

    /**
     * Expects two comma‐separated values: e.g. "lastupdatedDateTime,yyyy-MM-dd'T'HH:mm:ss:SSSZ"
     */
    @Value("${optimized.timeStampElementName}, ${optimized.timestampFormat}")
    private String[] timeStampElement;

    @Value("${enable.hash.comparison:false}")
    private boolean enableHashComparison;

    // --- Dependencies ---
    private final MongoTemplate mongoTemplate;
    private final ApplicationContext context;
    private final PersistenceJsontoMongoDb persistenceJsonToMongoDBd;
    private final PayloadRefactorService payloadRefactorService;
    private final ObjectMapper objectMapper;
    private final JsonHashUtils jsonHashUtils; // Utility for hash generation

    public ContractProcessingServiceImpl(MongoTemplate mongoTemplate,
                                         ApplicationContext context,
                                         PersistenceJsontoMongoDb persistenceJsonToMongoDBd,
                                         PayloadRefactorService payloadRefactorService,
                                         ObjectMapper objectMapper,
                                         JsonHashUtils jsonHashUtils) {
        this.mongoTemplate = mongoTemplate;
        this.context = context;
        this.persistenceJsonToMongoDBd = persistenceJsonToMongoDBd;
        this.payloadRefactorService = payloadRefactorService;
        this.objectMapper = objectMapper;
        this.jsonHashUtils = jsonHashUtils;
    }

    @Override
    public void createOptimizedPayload(String contractPayloadReceived) throws Exception {
        LOGGER.info("Started creating Optimized Payload");
        MdcInfo mdcInfo = new MdcInfo();
        AtomicReference<String> dbStatus = new AtomicReference<>();
        long startTime = System.currentTimeMillis();

        // Parse the input payload using Jackson
        JsonNode contractPayload = objectMapper.readTree(contractPayloadReceived);

        Optional<JsonNode> optContractId = extractValuesOutOfPayload(contractPayload, "contractIdentifier");
        Integer contractIdentifier = optContractId.map(JsonNode::asInt).orElse(null);
        LOGGER.info("Received payload contract Identifier is: {}", contractIdentifier);

        String hashValue = extractValuesOutOfPayload(contractPayload, PayloadConstants.HASH_VALUE)
                .map(JsonNode::asText).orElse("");
        String correlationIdentifier = extractValuesOutOfPayload(contractPayload, PayloadConstants.CORRELATION_IDENTIFER)
                .map(JsonNode::asText).orElse("");
        LOGGER.info("Received Contract payload HashValue is: {}", hashValue);
        LOGGER.info("Is Hash Comparison enabled: {}", enableHashComparison);
        LOGGER.info("Started parsing input payload to create optimized Payload");

        // Create optimized JSON payload container
        ObjectNode optimizedJSON = objectMapper.createObjectNode();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeStampElement[1]);
        optimizedJSON.put(timeStampElement[0], ZonedDateTime.now().format(formatter));

        LOGGER.info("Started processing the Type of Payload received");
        processContract(contractPayload, PayloadConstants.SWAP_CONTRACT, optimizedJSON, mdcInfo, contractIdentifier);
        processContract(contractPayload, PayloadConstants.CASH_CONTRACT, optimizedJSON, mdcInfo, contractIdentifier);

        LOGGER.info("Started extracting Attributes from Source Payload to create optimized Payload");
        addAttributesToOptimizedPayload(contractPayload, optimizedJSON, mdcInfo, attributes);
        addAttributesArrayToOptimizedPayload(contractPayload, optimizedJSON, attributesArray, mdcInfo);

        LOGGER.info("Optimized JSON: {}", optimizedJSON.toString());
        long timeTaken = System.currentTimeMillis() - startTime;
        LOGGER.info("Total time taken (in milliseconds) to create Optimized Payload: {}", timeTaken);

        Optional<JsonNode> extractedValue = extractValuesOutOfPayload(contractPayload, PayloadConstants.EVENT_NAME);
        if (extractedValue.isPresent() && extractedValue.get().asText().equalsIgnoreCase("CONTRACT_CANCELLED")) {
            LOGGER.info("Contract Cancelled Payload received. Record having ContractIdentifier: {} will be deleted in Mongo {} Collection",
                    contractIdentifier, mongoCollection);
            processContractDelete(optimizedJSON, contractIdentifier, "CONTRACT_CANCELLED");
        } else {
            persistData(optimizedJSON, dbStatus);
        }

        // Send non-critical messages asynchronously to Kafka
        sendDataToKafkaOutAsync(payloadRefactorService.payloadRefactor(contractPayloadReceived), mdcInfo, "Source Payload");
        sendDataToKafkaOutAsync(createAcknowledgementPayload(contractPayload), mdcInfo, "Acknowledgment Payload");

        long totalTime = System.currentTimeMillis() - startTime;
        mdcInfo.setDbProcessingTime(String.valueOf(totalTime));
        mdcInfo.setDbStatus(dbStatus.get());
        MdcUtil.saveMDC(mdcInfo);
        LOGGER.info("Completed creating Optimized payload and sent to Mongo, S3 and Kafka out topic");
        LOGGER.info("Total time taken (in milliseconds) to complete processing, persisting to Mongo, and sending to Kafka out: {}", totalTime);
    }

    private ObjectNode createAcknowledgementPayload(JsonNode contractPayload) {
        long startTime = System.currentTimeMillis();

        ObjectNode acknowledgementJson = objectMapper.createObjectNode();
        ObjectNode eventMetadata = objectMapper.createObjectNode();

        eventMetadata.put("producerName", "CAPP_Source");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss:SSSZ");
        eventMetadata.put("messageTimeStamp", ZonedDateTime.now().format(dtf));
        eventMetadata.put("versionNumber", extractValuesOutOfPayload(contractPayload, "versionNumber")
                .map(JsonNode::asText).orElse(""));
        eventMetadata.put("requestActionType", extractValuesOutOfPayload(contractPayload, "requestActionType")
                .map(JsonNode::asText).orElse(""));

        ObjectNode eventContainer = objectMapper.createObjectNode();
        eventContainer.put("eventIdentifier", extractValuesOutOfPayload(contractPayload, "eventIdentifier")
                .map(JsonNode::asText).orElse(""));
        eventContainer.put("entityType", extractValuesOutOfPayload(contractPayload, "entityType")
                .map(JsonNode::asText).orElse(""));
        eventContainer.put("entityIdentifier", extractValuesOutOfPayload(contractPayload, "entityIdentifier")
                .map(JsonNode::asText).orElse(""));
        eventContainer.put("persistenceStatus", true);

        ArrayNode eventContainerArray = objectMapper.createArrayNode();
        eventContainerArray.add(eventContainer);

        ObjectNode eventPayload = objectMapper.createObjectNode();
        eventPayload.set("eventContainer", eventContainerArray);

        ObjectNode events = objectMapper.createObjectNode();
        events.set("eventMetadata", eventMetadata);
        events.set("eventPayload", eventPayload);

        acknowledgementJson.set("events", events);
        LOGGER.info("Acknowledgment payload to be sent to Kafka Out: {}", acknowledgementJson.toString());

        long totalTimeForAckPayload = System.currentTimeMillis() - startTime;
        LOGGER.info("Total Time to create Acknowledgment payload for Kafka Out is: {}", totalTimeForAckPayload);

        return acknowledgementJson;
    }

    private void processContract(JsonNode contractPayload, String contractType, ObjectNode optimizedJSON,
                                 MdcInfo mdcInfo, Integer contractIdentifier) {
        if (contractType.equalsIgnoreCase(Payloads.LIVE_TRADE_CONTRACT) && isLiveTrade(contractPayload)) {
            LOGGER.info("Entered Live Trade. Extracting ContractIdentifier from: {}", PayloadConstants.LIVE_TRADE_IDENTIFIER);
            contractIdentifier = extractValuesOutOfPayload(contractPayload, PayloadConstants.LIVE_TRADE_IDENTIFIER)
                    .map(JsonNode::asInt)
                    .orElse(contractIdentifier);
            LOGGER.info("Contract Identifier extracted value: {}", contractIdentifier);
        }
        extractValuesOutOfPayload(contractPayload, contractType).ifPresent(contract -> {
            if (contract.isArray() && contract.size() > 0 && contract.get(0).isObject()) {
                contract = contract.get(0);
            }
            optimizedJSON.set(contractType, contract);
        });
        mdcInfo.setEventType(contractType);
        mdcInfo.setContractIdentifier(contractIdentifier);
    }

    private void addAttributesToOptimizedPayload(JsonNode contractPayload, ObjectNode optimizedJSON,
                                                  MdcInfo mdcInfo, String[] attributes) {
        LOGGER.info("Extracting attribute keys: {}", String.join(", ", attributes));
        for (String elementKey : attributes) {
            JsonNode elementValue = extractValuesOutOfPayload(contractPayload, elementKey)
                    .orElse(NullNode.getInstance());
            if (isLiveTrade(contractPayload) && elementKey.equalsIgnoreCase(PayloadConstants.CONTRACT_IDENTIFIER)) {
                elementValue = objectMapper.convertValue(mdcInfo.getContractIdentifier(), JsonNode.class);
                LOGGER.info("ContractIdentifier for Live Trade optimized will be: {}", elementValue.asText());
            }
            LOGGER.info("Adding Attribute Key: {} with value: {} to Optimized Payload", elementKey, elementValue);
            optimizedJSON.set(elementKey, elementValue);
        }
    }

    private void addAttributesArrayToOptimizedPayload(JsonNode contractPayload, ObjectNode optimizedJSON,
                                                      String[] attributesArray, MdcInfo mdcInfo) {
        LOGGER.info("Extracting attribute array keys: {}", String.join(", ", attributesArray));
        for (String elementKey : attributesArray) {
            Set<JsonNode> elementValueSet = extractValuesArrayOutOfPayload(contractPayload, elementKey);
            String result = elementValueSet.stream()
                    .map(JsonNode::asText)
                    .collect(Collectors.joining(", "));
            LOGGER.info("Adding Attribute Array Key: {} with value: {} to Optimized Payload", elementKey, result);
            ArrayNode elementValueArray = objectMapper.createArrayNode();
            elementValueSet.forEach(elementValueArray::add);
            optimizedJSON.set(elementKey, elementValueArray.size() == 0 ? NullNode.getInstance() : elementValueArray);
        }
    }

    /**
     * Recursively search for the elementKey in the JSON payload.
     */
    private Optional<JsonNode> extractValuesOutOfPayload(JsonNode node, String elementKey) {
        if (node.has(elementKey)) {
            return Optional.of(node.get(elementKey));
        }
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                Optional<JsonNode> found = extractValuesOutOfPayload(entry.getValue(), elementKey);
                if (found.isPresent()) {
                    return found;
                }
            }
        } else if (node.isArray()) {
            for (JsonNode arrayElement : node) {
                Optional<JsonNode> found = extractValuesOutOfPayload(arrayElement, elementKey);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Recursively extracts all values for targetKey in the JSON payload.
     */
    private Set<JsonNode> extractValuesArrayOutOfPayload(JsonNode sourceJson, String targetKey) {
        Set<JsonNode> resultSet = new HashSet<>();
        if (sourceJson.isObject()) {
            sourceJson.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                if (key.equals(targetKey) && (value.isTextual() || value.isNumber())) {
                    resultSet.add(value);
                } else {
                    resultSet.addAll(extractValuesArrayOutOfPayload(value, targetKey));
                }
            });
        } else if (sourceJson.isArray()) {
            for (JsonNode element : sourceJson) {
                resultSet.addAll(extractValuesArrayOutOfPayload(element, targetKey));
            }
        }
        return resultSet;
    }

    private boolean isLiveTrade(JsonNode contractPayload) {
        String productType = extractValuesOutOfPayload(contractPayload, PayloadConstants.PRODUCT_TYPE)
                .map(JsonNode::asText)
                .orElse("");
        return productType.equalsIgnoreCase(PayloadConstants.LIVE_TRADE_PRODUCT_TYPE);
    }

    private void compareReceivedPayloadHashtoEventHash(String receivedPayloadHashValue, JsonNode eventPayload) throws Exception {
        ObjectNode generateHashPayload = objectMapper.createObjectNode();
        generateHashPayload.set(PayloadConstants.EVENT_PAYLOAD, eventPayload);
        LOGGER.info("Started comparing the hash");
        String receivedPayloadGeneratedHashValue = jsonHashUtils.generateHash(generateHashPayload.toString());
        boolean match = receivedPayloadHashValue.equals(receivedPayloadGeneratedHashValue);
        if (enableHashComparison && match) {
            LOGGER.info("HASH Matched");
        } else {
            LOGGER.error("HASH Mismatch: JSON Data integrity failed");
            throw new JsonHashException("HASH Mismatch: JSON Data integrity failed");
        }
    }

    @Override
    public CompletableFuture<String> kafkaWrite(String inputJson, String contractProducerType) {
        LOGGER.info("Started writing input JSON into Kafka Topic: {}", contractProducerType);
        KafkaRegister kafkaRegister = new KafkaRegister(context);
        KafkaTemplate<String, String> template = kafkaRegister.getProducerKafkaTemplate(contractProducerType);
        CompletableFuture<SendResult<String, String>> future =
                template.send(kafkaRegister.getProducerTopic(contractProducerType), inputJson)
                        .completable(); // If conversion is required, convert ListenableFuture to CompletableFuture.
        LOGGER.info("Completed attempt to write JSON to topic for producer type: {}", contractProducerType);
        return future.thenApply(result -> "Successfully loaded input JSON to Topic")
                .exceptionally(ex -> "Failure to load JSON to topic: " + ex.getMessage());
    }

    @Async
    public void sendDataToKafkaOutAsync(String payload, MdcInfo mdcInfo, String payloadType) {
        try {
            long startTime = System.currentTimeMillis();
            LOGGER.info("Started asynchronous sending of {} to Kafka out topic", payloadType);
            CompletableFuture<String> future = kafkaWrite(payload, "contracteventouttopic");
            future.thenAccept(msg -> LOGGER.info("Kafka Out {} result: {}", payloadType, msg));
            long totalTime = System.currentTimeMillis() - startTime;
            mdcInfo.setKafkaProcessingTime(String.valueOf(totalTime));
            mdcInfo.setKafkaStatus("success");
            LOGGER.info("Total time taken (in milliseconds) to send {} into Kafka out: {}", payloadType, totalTime);
        } catch (KafkaProducerException e) {
            LOGGER.error("Exception while sending payload to Kafka out topic: {}", e.getMessage());
            mdcInfo.setKafkaStatus("failure");
            throw new ContractPayloadProcessingException("Exception while sending payload to Kafka out topic", e);
        }
    }

    @Async
    public void sendDataToKafkaOutAsync(JsonNode payload, MdcInfo mdcInfo, String payloadType) {
        sendDataToKafkaOutAsync(payload.toString(), mdcInfo, payloadType);
    }

    private void persistData(JsonNode optimizedJSON, AtomicReference<String> dbStatus) {
        try {
            long startTime = System.currentTimeMillis();
            LOGGER.info("Started persisting JSON to Mongo DB");
            ArrayList<String> emptyList = new ArrayList<>();
            String payloadInsertionUpdateStatusMessage = persistenceJsonToMongoDBd
                    .processData(optimizedJSON.toString(), mongoCollection, mongoField, emptyList);
            LOGGER.info(payloadInsertionUpdateStatusMessage);
            String payloadInsertionMessage = persistenceJsonToMongoDBd
                    .insertToMongoCollection(optimizedJSON.toString(), mongoCollectionTimetravel);
            LOGGER.info(payloadInsertionMessage);
            LOGGER.info("Contract data persisted to DB");
            dbStatus.set("SUCCESS");
            long totalTimeToPersist = System.currentTimeMillis() - startTime;
            LOGGER.info("Total time taken (in milliseconds) to persist Optimized payload to mongo: {}", totalTimeToPersist);
        } catch (Exception e) {
            LOGGER.error("Exception while persisting payload to mongo: {}", e.getMessage());
            dbStatus.set("failure");
            throw new ContractPayloadProcessingException("Exception while persisting payload to mongo", e);
        }
    }

    public void processContractDelete(JsonNode optimizedJSON, Integer contractId, String eventType) {
        try {
            MongoCollection<Document> collection = mongoTemplate.getCollection(mongoCollection);
            Document query = collection.find(Filters.eq(PayloadConstants.CONTRACT_IDENTIFIER, contractId)).first();
            if (query != null) {
                collection.deleteOne(query);
                LOGGER.info("Completed Processing {} event. Deleted record with contract identifier: {}", eventType, contractId);
                String payloadInsertionMessage = persistenceJsonToMongoDBd
                        .insertToMongoCollection(optimizedJSON.toString(), mongoCollectionTimetravel);
                LOGGER.info(payloadInsertionMessage);
            } else {
                LOGGER.info("No Document found with contract ID {} to perform delete for event type: {}", contractId, eventType);
            }
        } catch (Exception e) {
            LOGGER.error("Exception while processing contract delete: {}", e.getMessage());
        }
    }
}
