package com.jdann.aws.wc;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class WordCountHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(WordCountHandler.class);

    private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    private static final String DYNAMODB_TABLE_NAME = "word_totals";
    private static final String WORD_COUNT_QUEUE = "https://sqs.us-west-2.amazonaws.com/736338261372/word-count";
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    public Response handler(Request request) {

        Response response = new Response();
        getFromDynamoOrQueue(request.getAddress(), response);
        return response;
    }

    private void getFromDynamoOrQueue(String address, Response response) {

        response.setWords(new ArrayList<>());

        if (address == null) {
            response.setStatus("Error. URL can't be null");
            LOGGER.error("NULL_URL");
            return;
        }

        if (!doesURLExist(address)) {
            response.setStatus("Error. Invalid URL: " + address);
            LOGGER.error("INVALID_URL {}", address);
            return;
        }

        //check if already in database
        List<Word> words = findAllWithHashKey(address);

        System.out.println("words:" + words);
        if (!words.isEmpty()) {

            words.sort(Word.byTotal);
            response.setStatus("Success");
            response.setWords(words);
            LOGGER.info("SUCCESS_DATABASE: {}", address);

        } else {

            //put in queue and return processing message
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(WORD_COUNT_QUEUE)
                    .withMessageBody(address);

            sqs.sendMessage(send_msg_request);
            response.setStatus("Successfully added '" + address + "' to queue, please try again later for results.");
            response.setWords(words);
            LOGGER.info("SUCCESS_QUEUED: {}", address);

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

        //marshall the results into WordTotals
        return result.getItems().stream().map(this::getWordFromItem).collect(Collectors.toList());
    }

    private Word getWordFromItem(Map<String, AttributeValue> item) {
        Word word = new Word();

        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {

            switch (entry.getKey()) {
                case "address" : word.setAddress(entry.getValue().getS()); break;
                case "word"    : word.setWord(entry.getValue().getS()); break;
                case "total"   : word.setTotal(Long.valueOf(entry.getValue().getN())); break;
            }
        }
        return word;
    }

    private boolean doesURLExist(String address) {

        try {

            URL url = new URL(address);

            //check the current URL
            HttpURLConnection.setFollowRedirects(false);

            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            //don't need to get data
            httpURLConnection.setRequestMethod("HEAD");
            //some websites don't like programmatic access so pretend to be a browser
            httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
            int responseCode = httpURLConnection.getResponseCode();
            //only accept response code 200
            return responseCode == HttpURLConnection.HTTP_OK;

        } catch (IOException e) {
            return false;
        }
    }

    public static class Request {

        private String address;

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }

    public static class Response {

        private String status;

        private List<Word> words;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public List<Word> getWords() {
            return words;
        }

        public void setWords(List<Word> words) {
            this.words = words;
        }

    }

    public static class Word {

        private String address;
        private String word;
        private Long total;

        public Word(String address, String word) {
            this.address = address;
            this.word = word;
            this.total = 0L;
        }

        public Word() {}

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Long getTotal() {
            return total;
        }

        public void setTotal(Long total) {
            this.total = total;
        }

        public static Comparator<Word> byTotal = (Word w1, Word w2) -> w2.getTotal().compareTo(w1.getTotal());
    }

}
