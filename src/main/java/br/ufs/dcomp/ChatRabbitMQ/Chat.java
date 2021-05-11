package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.UnsupportedEncodingException;
import java.io.IOException;

import java.util.Scanner;
import java.util.Calendar; 

public class Chat {
  public static String command, user, shell, queue_name, exchange_name = "", destination = "";  
  public static Scanner in = new Scanner(System.in);
    
  private static byte[] serializeData(byte[] buffer) throws IOException{
    Calendar calendar = Calendar.getInstance();
    final String DATA_SEND = calendar.get(Calendar.DAY_OF_MONTH) + "/" 
                    + (calendar.get(Calendar.MONTH) + 1) + "/" 
                    + calendar.get(Calendar.YEAR), 
                 HOUR_SEND = calendar.get(Calendar.HOUR_OF_DAY) + ":"
                    + calendar.get(Calendar.MINUTE);
    
    MessageData.Content.Builder content = MessageData.Content.newBuilder(); 
    content.setBody(ByteString.copyFrom(buffer));
    
    MessageData.Message.Builder raw = MessageData.Message.newBuilder();
    raw.setSender(user);
    raw.setDate(DATA_SEND);
    raw.setHour(HOUR_SEND); 
    raw.setContent(content);
    
    return raw.build().toByteArray();
  }
  
  private static String deserializeData(byte[] body) throws InvalidProtocolBufferException {
    MessageData.Message message_data = MessageData.Message.parseFrom(body);
    MessageData.Content content = message_data.getContent();
    String displayed_message = ""; 
    
    String message = content.getBody().toStringUtf8();
    displayed_message = "(" + message_data.getDate() + " às " 
                        + message_data.getHour() + ") " + message_data.getSender() 
                        + " diz: " + message; 
    
    
    return displayed_message;
  }
   
  private static void getCommand(Channel channel) throws UnsupportedEncodingException, IOException { 
    char prefix = command.trim().charAt(0);
     
    if (prefix == '@') { 
      shell = command.trim() + ">> ";
      destination = command.trim().substring(1);
      exchange_name = "";
    } 
    else { 
        channel.basicPublish(exchange_name, destination, null, serializeData(command.getBytes("UTF-8"))); 
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
    
    channel.basicConsume(queue_name, true,    consumer); 
  }
}