import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class worker {
    private static Region region = Region.of("us-east-1");
    private static SqsClient sqs = SqsClient.builder().region(region).build();
    private static Map<String, MessageAttributeValue> message_para= new HashMap<String,MessageAttributeValue>(); //sqs message format

    public static void main(String[] args) {
        try{
            while(true) {
                System.out.println("worker listen to queues");
                listen_to_queues();
                try {
                    Thread.currentThread().sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch(Exception e){System.out.println(e);
            try {
                Thread.sleep(4000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void send_error_message(Message original_message,String queue_url,String error){
        System.out.println("worker incounter an error "+ error);
        message_para.put("worker_instance_id",MessageAttributeValue.builder().stringValue(EC2MetadataUtils.getInstanceId()).dataType("String").build());
        send_message("error",original_message,queue_url,error);
    }

    private static void send_message(String action,Message original_message,String queue_url,String body) {
        try {
            message_para.put("action",MessageAttributeValue.builder().stringValue(action).dataType("String").build());
            message_para.put("to",MessageAttributeValue.builder().stringValue(String.valueOf(original_message.messageAttributes().get("from"))).dataType("String").build());
            message_para.put("from",MessageAttributeValue.builder().stringValue(String.valueOf(original_message.messageAttributes().get("to"))).dataType("String").build());
            if(queue_url != null) {
                SendMessageRequest request = SendMessageRequest.builder().queueUrl(queue_url).messageBody(body).messageAttributes(message_para).build();
                sqs.sendMessage(request);
            }else {
                System.out.println("Could not find queue");
            }
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void listen_to_queues(){
        for(String url : sqs.listQueues().queueUrls()){
            if(url.contains("worker")&&url.contains("newtask"))
                check_queue_for_messages(url);
        }
    }

    private static void check_queue_for_messages(String queue_url) {
        try {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(queue_url).messageAttributeNames("All").maxNumberOfMessages(1).waitTimeSeconds(5).build();
            List<Message> messages = sqs.receiveMessage(request).messages();
            check_messages(messages,queue_url);
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void check_messages(List<Message> messages,String queue_url) {
        MessageAttributeValue to_value = MessageAttributeValue.builder().stringValue("worker").dataType("String").build();
        MessageAttributeValue action_value = MessageAttributeValue.builder().stringValue("new image task").dataType("String").build();
        for(Message message:messages){
            if(message.messageAttributes().get("to").equals(to_value) && message.messageAttributes().get("action").equals(action_value)) {
                System.out.println("worker process task and change message visibility");
                change_message_visibility(message,queue_url);
                System.out.println("worker process task");
                process_message(message,queue_url);
                break;
            }
        }
    }

    private static void change_message_visibility(Message message, String queue_url) {
        try {
            ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder().queueUrl(queue_url)
                    .receiptHandle(message.receiptHandle()).visibilityTimeout(5).build();
            sqs.changeMessageVisibility(request);
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            send_error_message(message,queue_url,e.awsErrorDetails().errorMessage());
        }
    }

    private static void process_message(Message message, String queue_url) {
        System.out.println("worker downlaod image from url");
        String done_queue_url = get_done_queue_url(message.messageAttributes().get("from").stringValue());
        Image img = download_from_url(message);
        if(img != null) {
            System.out.println("worker ocr image");
            String[] result = ocr_image(img); // [result,error]
            if(result[0] != null) {
                System.out.println("worker delete message");
                String delete_error = delete_message_from_queue(message,queue_url);
                if(delete_error == null) {
                    System.out.println("worker update manger");
                    update_manger(result[0], message, done_queue_url);
                }
                else{
                    send_error_message(message,done_queue_url,delete_error);
                }
            }
            else{
                send_error_message(message,done_queue_url,result[1]);
            }
        }
        else{
            send_error_message(message,done_queue_url,"Could not download image");
        }
    }

    private static String get_done_queue_url(String from) {
        for(int i=0;i<3;i++) {
            for (String queue_urls : sqs.listQueues().queueUrls()) {
                if (queue_urls.contains(from) && queue_urls.contains("donetask"))
                    return queue_urls;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static String delete_message_from_queue(Message message, String queue_url) {
        String error= null;
        try{
            DeleteMessageRequest request = DeleteMessageRequest.builder().queueUrl(queue_url).receiptHandle(message.receiptHandle()).build();
            sqs.deleteMessage(request);
        }catch (SqsException e){
            System.err.println(e.awsErrorDetails().errorMessage());
            error = e.awsErrorDetails().errorMessage();
        }
        return error;
    }

    private static void update_manger(String result, Message message, String queue_url) {
        String msg_body = message.body()+ System.lineSeparator()+result;
        send_message("done OCR task",message,queue_url,msg_body);
    }

    private static String[] ocr_image(Image img) {
        String result = null;
        String error = null;
        String[] arr = new String[2];
        ITesseract tessj4 = new Tesseract();  // JNA Interface Mapping
        tessj4.setDatapath("./tessdata"); // path to tessdata directory
        tessj4.setTessVariable("user_defined_dpi", "70");
        try {
             result = tessj4.doOCR((BufferedImage) img);
        } catch (TesseractException e) {
            error = e.getMessage();
        }
        arr[0] = result;arr[1] = error;
        return arr;
    }

    private static Image download_from_url(Message message) {
        return download_image_from_web(message.body());
    }

    private static Image download_image_from_web(String image_url) {
        Image image = null;
        try {
            URL url = new URL(image_url);
            image = ImageIO.read(url);
        } catch (IOException e) {}
        return image;
    }
}
