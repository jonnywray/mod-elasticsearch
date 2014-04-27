/*
 * Copyright 2013 Jonny Wray
 *
 * @author <a href="http://www.jonnywray.com">Jonny Wray</a>
 */

package com.jonnywray.vertx.elasticsearch;


import io.netty.handler.codec.http.HttpHeaders;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.UnsupportedEncodingException;

/**
 * {@link org.vertx.java.platform.Verticle} that uses Elastic Search as a JSON store using the HTTP interface to Elastic Search.
 *
 * @author Jonny Wray
 */
public class ElasticSearchPersistor extends BusModBase implements Handler<Message<JsonObject>> {

    private static final String JSON_CONTENT_TYPE = "application/json";
    protected String address;
    protected String host;
    protected int port;

    protected HttpClient client;

    @Override
    public void start() {
        super.start();
        address = getOptionalStringConfig("address", "jonnywray.elasticsearch");
        host = getOptionalStringConfig("host", "localhost");
        port = getOptionalIntConfig("port", 9200);
        try{
            client = vertx.createHttpClient()
                    .setPort(port)
                    .setHost(host)
                    .setKeepAlive(true)
                    .setSSL(false);
            JsonArray indices = container.config().getArray("indices");
            if(indices != null){
                for(int i=0;i<indices.size();i++){
                    JsonObject indexConfiguration = indices.get(i);
                    // TODO: check for index existence first
                    createIndex(indexConfiguration);
                }
            }
            vertx.eventBus().registerHandler(address, this);
            container.logger().info("successfully started Elastic Search persistor verticle");
        }
        catch (Exception e){
            container.logger().error("error starting Elastic Search persistor verticle", e);
            throw e;
        }
    }

    @Override
    public void stop() {
        super.stop();
        client.close();
    }

    @Override
    public void handle(final Message<JsonObject> message) {
        String action = message.body().getString("action");
        if (action == null) {
            sendError(message, "action must be specified");
            return;
        }
        switch (action){
            case "index" :
                index(message);
                break;
            default:
                sendError(message, "unsupported action specified: "+action);
        }
    }


    private void createIndex(JsonObject indexConfiguration){
        final String index = indexConfiguration.getString("index");
        JsonObject configuration = indexConfiguration.getObject("configuration");
        String createIndexUri = index;
        HttpClientRequest request = client.post(createIndexUri, new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode >= 200 && responseCode <= 300) {
                             container.logger().info("Successfully created index "+index);
                        }
                        else{
                            String errorMessage =  "error creating index: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                        }
                    }
                });
            }
        });
        writeObject(request, configuration);
    }

    private void index(final Message<JsonObject> message) {
        String index = message.body().getString("index");
        String type = message.body().getString("type");
        String id = message.body().getString("id");
        String indexUri = index + "/" + type;
        if(id != null){
            indexUri = indexUri + "/" + id;
        }
        Handler<HttpClientResponse> handler = new Handler<HttpClientResponse>() {
            @Override
            public void handle(final HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        int responseCode = response.statusCode();
                        if (responseCode >= 200 && responseCode <= 300) {
                            JsonObject responseObject = new JsonObject(body.toString());
                            sendOK(message, responseObject);
                        }
                        else{
                            String errorMessage =  "error indexing object: " + response.statusCode() + " " + response.statusMessage();
                            container.logger().error(errorMessage);
                            sendError(message, errorMessage);
                        }
                    }
                });
            }
        };
        HttpClientRequest request = id == null ? client.post(indexUri, handler) : client.put(indexUri, handler);
        JsonObject jsonObject = message.body().getObject("object");
        writeObject(message, request, jsonObject);
    }


    private void writeObject(Message<JsonObject> message, HttpClientRequest request, JsonObject object){
        try{
            String encodedObject = object.encode();
            request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                    .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes("UTF-8").length))
                    .write(encodedObject)
                    .end();
        }
        catch (UnsupportedEncodingException e){
            container.logger().error("error converting JSON objects to byte[] with UTF-8 encoding", e);
            sendError(message, "unable to encode command body");
        }
    }

    private void writeObject(HttpClientRequest request, JsonObject object){
        try{
            String encodedObject = object.encode();
            request.putHeader(HttpHeaders.Names.CONTENT_TYPE, JSON_CONTENT_TYPE)
                    .putHeader(HttpHeaders.Names.CONTENT_LENGTH, Integer.toString(encodedObject.getBytes("UTF-8").length))
                    .write(encodedObject)
                    .end();
        }
        catch (UnsupportedEncodingException e){
            container.logger().error("error converting JSON objects to byte[] with UTF-8 encoding", e);
        }
    }


}
