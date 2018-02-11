package com.jdann.aws.wc;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.jdann.aws.wc.dto.Request;
import com.jdann.aws.wc.dto.Response;
import com.jdann.aws.wc.dto.Word;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountHandler implements RequestHandler<Request, Response> {

    private AmazonDynamoDBClient client;
    private DynamoDB dynamoDb;
    private static final String DYNAMODB_TABLE_NAME = "word_totals";
    private static final Regions REGION = Regions.US_WEST_2;
    private static final String WORD_COUNT_QUEUE = "https://sqs.us-west-2.amazonaws.com/736338261372/word-count";
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    public Response handleRequest(Request request, Context context) {

        initDynamoDbClient();

        Response response = new Response();
        getFromDynamoOrQueue(request.getAddress(), response);
        return response;
    }

    private void getFromDynamoOrQueue(String address, Response response) {

        if (address == null) {
            response.setStatus("Error: Please enter a valid URL.");
            response.setWords(new ArrayList<>());
            return;
        }

        //check if already in database
        List<Word> words = findAllWithHashKey(address);

        System.out.println("words:" + words);
        if (!words.isEmpty()) {

            words.sort(Word.byTotal);
            response.setStatus("Success");
            response.setWords(words);

        } else {

            //put in queue and return processing message
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(WORD_COUNT_QUEUE)
                    .withMessageBody(address);

            sqs.sendMessage(send_msg_request);

            response.setStatus("Successfully added '" + address + "' to queue, please try again later for results.");
            response.setWords(words);
        }
    }

    private List<Word> findAllWithHashKey(String hashKey) {

        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.put(":val", new AttributeValue().withS(hashKey));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(DYNAMODB_TABLE_NAME)
                .withFilterExpression("address = :val")
                .withExpressionAttributeValues(attributeValues);

        ScanResult result = client.scan(scanRequest);
        List<Word> words = new ArrayList<>();

        //marshall the results into WordTotals
        result.getItems().forEach(item -> {

            Word word = new Word();
            item.forEach((k, v) -> {
                switch (k) {
                    case "address"   : word.setAddress(v.getS()); break;
                    case "word"      : word.setWord(v.getS()); break;
                    case "total"     : word.setTotal(Long.valueOf(v.getN())); break;
                }
            });
            words.add(word);
        });
        return words;
    }

    private void initDynamoDbClient() {
        client = new AmazonDynamoDBClient();
        client.setRegion(Region.getRegion(REGION));
        this.dynamoDb = new DynamoDB(client);
    }
}
