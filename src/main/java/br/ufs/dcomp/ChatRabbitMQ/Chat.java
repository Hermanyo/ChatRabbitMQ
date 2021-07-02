package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.File; 
import java.io.UnsupportedEncodingException;
import java.io.IOException;

import java.util.Scanner;
import java.util.Calendar; 
import java.util.Base64;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileSystemNotFoundException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Chat {
  public static String command, user, shell, queue_name, exchange_name = "", destination = "";  
  public static Scanner in = new Scanner(System.in);
    
  private static byte[] serializeData(byte[] buffer, String mime_type, String file_name ) throws IOException{
    Calendar calendar = Calendar.getInstance();
    final String DATA_SEND = calendar.get(Calendar.DAY_OF_MONTH) + "/" 
                    + (calendar.get(Calendar.MONTH) + 1) + "/" 
                    + calendar.get(Calendar.YEAR), 
                 HOUR_SEND = calendar.get(Calendar.HOUR_OF_DAY) + ":"
                    + calendar.get(Calendar.MINUTE);
    
    MessageData.Content.Builder content = MessageData.Content.newBuilder(); 
    content.setType(mime_type);
    content.setName(file_name);
    content.setBody(ByteString.copyFrom(buffer));
    
    MessageData.Message.Builder raw = MessageData.Message.newBuilder();
    raw.setSender(user);
    raw.setDate(DATA_SEND);
    raw.setHour(HOUR_SEND); 
    raw.setContent(content);
    raw.setGroup(exchange_name);

    return raw.build().toByteArray();
  }
  
  private static String deserializeData(byte[] body) throws InvalidProtocolBufferException {
    MessageData.Message message_data = MessageData.Message.parseFrom(body);
    MessageData.Content content = message_data.getContent();
    String displayed_message = ""; 
    
    String message = content.getBody().toStringUtf8();
    
    if(content.getType().equals("text/plain")){
      if(message_data.getGroup().equals("")){
        displayed_message = "(" + message_data.getDate() + " às " 
                            + message_data.getHour() + ") " + message_data.getSender() 
                            + " diz: " + message; 
      }
      else{
        displayed_message = "(" + message_data.getDate() + " às " 
                            + message_data.getHour() + ") " + message_data.getSender() 
                            + "#" + message_data.getGroup() + " diz: " + message;
      }
    }
    else{
      String file_name = content.getName();
      byte[] file_buffer = content.getBody().toByteArray();
      String DEFAULT_DOWNLOAD_FOLDER = "/home/ubuntu/environment/chat/downloads/";

      try {
 
        File folder = new File(DEFAULT_DOWNLOAD_FOLDER + "/" + user);
        File file = new File(folder.getPath() + "/" + file_name);
        
        folder.mkdirs();
        file.createNewFile();
        Files.write(file.toPath(), file_buffer);

        displayed_message = "(" + message_data.getDate() + " às " 
          + message_data.getHour() + ") "
          + "Arquivo " + file_name 
          + " recebido de " + "@" + message_data.getSender() 
          + (!message_data.getGroup().equals("") 
            ? " em " +  "#" + message_data.getGroup() + "!" 
            : "!");
      }
      catch (IOException io_e) {
        System.err.println(io_e);
      } 
    }
    return displayed_message;
  }
     private static String getValue(JSONObject obj, String key) {
    String value = (String) obj.get(key);
    
    return (
            !(value.equals("") || 
            (key.equals("destination") && value.matches("(.*)_files(.*)")))
           )
          ? value + ", " 
          : "";
  }
  
  private static void doRequest(String path, String json_key) { 
    try {
      Client client = ClientBuilder.newClient();
      Response res = client.target("http://target-group-amqp-df4b5ea3fe3cc2e7.elb.us-east-1.amazonaws.com/")
                  .path(path)
                  .request(MediaType.APPLICATION_JSON)
                  .header("Authorization", "Basic " + Base64.getEncoder().encodeToString("admin:sd2543".getBytes()))
                  .get();
                  
      if (res.getStatus() == 200) {
        String json = res.readEntity(String.class);
        JSONParser json_parser = new JSONParser();
      	Object obj = json_parser.parse(json);
      	JSONArray json_array = (JSONArray) obj;
      	
      	json_array.forEach(elem -> 
	        System.out.print(
	           getValue((JSONObject) elem, json_key)
	        )
      	);
      	
      	System.out.println("");
      }
      else {
        System.out.println(res.getStatus());
      }
    } 
    catch(Exception e) {
      e.printStackTrace();
    }
  }
  private static void getCommand(Channel channel) throws UnsupportedEncodingException, IOException { 
    char prefix = command.trim().charAt(0);
     
    if (prefix == '@') { 
      shell = command.trim() + ">> ";
      destination = command.trim().substring(1);
      exchange_name = "";
    } 
    else if(prefix == '!'){
      String[] command_parts = command.split(" ");
      String group_command = command_parts[0].substring(1);
        
      if (group_command.equals("newGroup") || group_command.equals("addGroup") ) {
        channel.exchangeDeclare(command_parts[1], "direct");
        channel.queueBind(user, command_parts[1], "");  
        channel.queueBind(user + "_files", command_parts[1], "files");
      } 
      else if (group_command.equals("toGroup") || group_command.equals("addUser")) {
        channel.queueBind(command_parts[1], command_parts[2], ""); 
        channel.queueBind(command_parts[1] + "_files", command_parts[2], "files");
      }
      else if (group_command.equals("delFromGroup")) {
        channel.queueUnbind(command_parts[1], command_parts[2], "");  
        channel.queueUnbind(command_parts[1] + "_files", command_parts[2], "files");
        if (command_parts[1].equals(user)) {
          shell = ">> ";
          exchange_name = "";
        }
      }
      else if (group_command.equals("removeGroup")) {
        channel.exchangeDelete(command_parts[1]); 
        if (!exchange_name.equals("")) {
          shell = ">> ";
          exchange_name = "";
        }
      }
      else if (group_command.equals("upload")) {
        if (!exchange_name.equals("") || !destination.equals("")) {
          String outputMessage = "Enviando " 
                    + command_parts[1]
                    + " para " 
                    + "@" + destination;
          
          System.out.println(outputMessage);
          
          try {
            Path source = Paths.get(command_parts[1]);
            
            Runnable uploader = new Runnable() {
              public void run() {
                try {
                  String[] path_array = command_parts[1].split("/");
                  String mime_type = Files.probeContentType(source);
                  String file_name = path_array[path_array.length - 1];
                  byte[] file_buffer = Files.readAllBytes(source);
                  
                  channel.basicPublish(
                      exchange_name, 
                      destination.equals("") ? "files" : destination + "_files", 
                      null, 
                      serializeData(file_buffer, mime_type, file_name)
                  );
                  System.out.println("\n"
                    + "Arquivo " + command_parts[1] 
                    + " foi enviado para " 
                    + "@" + destination  
                  );  
                }
                catch(Exception e) {
                  System.err.println(e);
                }
                finally{
                   System.out.print(shell);
                }
              }
            };
            
            new Thread(uploader).start();
          }  
          catch(Exception e) { 
            System.err.println("Erro " + e);
          }
        }
      }
      else if (group_command.equals("listUsers")) {
          doRequest("/api/exchanges/%2F/"+ command_parts[1] +"/bindings/source", "destination");
      }
      else if (group_command.equals("listGroups")) {
        doRequest("/api/queues/%2F/"+ user +"/bindings", "source");
      }
      else {
        System.err.println("Comando inválido!");
      }
    }
    else if (prefix == '#') {
      shell = command.trim() + ">> ";
      exchange_name = command.trim().substring(1);
      destination = "";
    }
    else {  
        channel.basicPublish(exchange_name, destination, null, serializeData(command.getBytes("UTF-8"), "text/plain", null));
    } 
  }
  
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory(); 
    
    factory.setHost("172.31.27.164");
    factory.setUsername("admin");
    factory.setPassword("sd2543");
    factory.setVirtualHost("/");
    
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    
    System.out.print("User: ");
    user = in.nextLine();
    
    queue_name = user;
    shell = ">> ";  
    
    channel.queueDeclare(queue_name, false,false,false,null);  
    channel.queueDeclare(queue_name + "_files", false, false, false, null);

    Runnable supplier = new Runnable() {
      public void run() {
        try {
          while (true) {
            System.out.print(shell);
            command = in.nextLine();
            
            getCommand(channel);
          }
        } catch(Exception e) {}
      }
    };
    
    Consumer consumer = new DefaultConsumer(channel) {
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body){
        String message = "";
        
        try {
          message = deserializeData(body);
        } 
        catch(InvalidProtocolBufferException e) {}
        
        System.out.println("\n" + message);
        System.out.print(shell);
      }
    };
    
    Thread th = new Thread(supplier);
    th.start();
    
    channel.basicConsume(queue_name, true, consumer); 
    channel.basicConsume(queue_name + "_files", true,    consumer);
  }
}