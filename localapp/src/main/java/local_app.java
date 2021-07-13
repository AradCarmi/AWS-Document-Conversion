import com.sun.javafx.iio.ImageStorage;
import org.relaxng.datatype.Datatype;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import sun.plugin.dom.html.HTMLImageElement;

import javax.swing.text.html.HTML;
import java.io.*;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class local_app {
    private static String clientId;
    private static Region region = Region.of("us-east-1");
    private static Ec2Client ec2 = Ec2Client.builder().region(region).build();
    private static S3Client s3 = S3Client.builder().region(region).build();
    private static SqsClient sqs = SqsClient.builder().region(region).build(); // message_body_format = "from to action"
    private static String objects_name = "-manger";
    private static Boolean close_manger = true;
    private static Map<String,MessageAttributeValue> message_para= new HashMap<String,MessageAttributeValue>(); //sqs message format
    private static String queue_url;
    private static String image_id ="ami-04ed91c3c33da96fa";
    private static String num_of_workers ="";
    private static String arn = System.getenv("arn");
    private static String user_data;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        System.out.println("local app is starting");
        pre_process(args);
        process();
        done_process();
    }

    private static void pre_process(String[] args) {
        init(args);
        System.out.println("local app is reading input file");
        File input = get_user_input(args[0]);
        if (!is_manger_alive()){
            System.out.println("local app waking up the manger");
            wake_manger();
        }
        System.out.println("local app upload input file to s3");
        upload_file_to_s3(input);
    }

    private static void process() throws InterruptedException {
        System.out.println("local app sending start message");
        send_sqs_start_message();
        System.out.println("local app waiting to receive done message");
        while (!sqs_is_done()) {
            Thread.currentThread().sleep(2000);
        }
        System.out.println("local app received done message");
    }

    private static void done_process() throws FileNotFoundException {
        File file = download_summary_from_s3();
        output_summry_to_html(file);
        if (close_manger)
            send_stop_message_to_manger();
    }

    private static void output_summry_to_html(File file) {
        String line;
        File output = new File("./","output"+clientId.charAt(clientId.length()-1)+".html");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            OutputStream os = new FileOutputStream(output);
            os.write("<body>".getBytes());
            while ((line = br.readLine()) != null) {
                if(line.contains("http")) {
                    os.write("<p>".getBytes());
                    line = "<img src="+line+"><br/>";
                    os.write(line.getBytes());
                }
                else{
                    os.write(line.getBytes());
                    os.write("</p>".getBytes());
                }
            }
            os.write("</body>".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void init(String[] args) {
        clientId = System.getenv("client_id");
        num_of_workers = args[1];
        user_data = new String(Base64.getEncoder().encode((
                "#!/bin/bash -xe\n"+
                        "exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1 \n"+
                        "yum -y update\n"+
                        "cd home/ec2-user/manger\n"+
                        "java -jar manger.jar "+arn
        ).getBytes()));
        if (args.length < 3)
            close_manger = false;
        message_para.put("from",MessageAttributeValue.builder().stringValue("localapp").dataType("String").build());
        message_para.put("to",MessageAttributeValue.builder().stringValue("manger").dataType("String").build());
    }

    private static void send_stop_message_to_manger() {
        send_msg_to_manger("stop");
    }

    private static File download_summary_from_s3() throws FileNotFoundException {
        byte[] data = null;
        File temp = new File("./","temp.txt");
        OutputStream os = new FileOutputStream(temp);
        try{
            String bucket_name = clientId+objects_name;
            String key = "summary"+clientId.substring(clientId.length()-1)+".html";
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket_name).key(key).build();
            ResponseBytes<GetObjectResponse> respone =  s3.getObjectAsBytes(request);
            data = respone.asByteArray();
            os.write(data);
        }catch (S3Exception e){
            e.awsErrorDetails().errorMessage();
            System.exit(1);
        }catch (IOException e){}
        return temp;
    }

    private static boolean sqs_is_done() {
        try {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(queue_url).messageAttributeNames("All").build();
            List<Message> messages = sqs.receiveMessage(request).messages();
            for(Message message : messages){
                if(message.messageAttributes().get("action").stringValue().equals("done")) {
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queue_url).receiptHandle(message.receiptHandle()).build();
                    sqs.deleteMessage(deleteMessageRequest);
                    return true;
                }else{Thread.sleep(2000);}
            }
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void send_sqs_start_message() {
        String queue_name = clientId + objects_name;
        create_queue(queue_name);
        send_start_msg_to_manger();
    }

    private static void send_msg_to_manger(String action){
        try {
            message_para.put("action",MessageAttributeValue.builder().stringValue(action).dataType("String").build());
            SendMessageRequest request = SendMessageRequest.builder().queueUrl(queue_url).messageBody(num_of_workers).messageAttributes(message_para).build();
            sqs.sendMessage(request);
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void send_start_msg_to_manger() {
        send_msg_to_manger("start");
    }

    private static void create_queue(String queue_name) {
        String url=null;
        try {
            Map<QueueAttributeName,String> map = new HashMap<QueueAttributeName, String>();
            map.put(QueueAttributeName.fromValue("VisibilityTimeout"),"5");
            CreateQueueRequest request = CreateQueueRequest.builder().queueName(queue_name).attributes(map).build();
            url = sqs.createQueue(request).queueUrl();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        queue_url = url;
    }

    private static void upload_file_to_s3(File input) {
        String bucket_name = createBucket();
        if(bucket_name == null)
            System.exit(1);
        upload_to_bucket(bucket_name,input);
    }

    private static void upload_to_bucket(String bucket_name, File input) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket_name).key("input.txt").build();
        try{
            s3.putObject(request, RequestBody.fromFile(input));
        }catch (S3Exception e){
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static String createBucket() {
        try {
            String bucket_name = clientId + objects_name;
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucket_name)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build();
            s3.createBucket(bucketRequest);
            return bucket_name;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //clientId = clientId+clientId; // incase of error try to create new with double clientid
            //createBucket();
        }
        return null;
    }

    private static void wake_manger() {
        StartInstancesRequest request;
        Tag tag = Tag.builder().key("Manger").value("Manger").build();
        Instance inst = get_instance_by_tag(tag);
        if(inst != null) {
            try {
                request = StartInstancesRequest.builder().instanceIds(inst.instanceId()).build();
                ec2.startInstances(request);
            }catch(Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }

    private static File get_user_input(String path) {
        File file = new File(path);
        return file;
    }

    private static boolean is_manger_alive() {
        Tag tag = Tag.builder().key("Manger").value("Manger").build();
        Instance inst = get_instance_by_tag(tag);
        if(inst == null || inst.state().name().equals(InstanceStateName.TERMINATED))
            inst = create_manger();
        if(inst == null || inst.state().name().equals(InstanceStateName.STOPPED))
            return false;
        return true;
    }

    private static Instance create_manger() {
        RunInstancesRequest request = RunInstancesRequest.builder()
                .keyName("Manger_key")
                .imageId(image_id)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(arn).build()).instanceType(InstanceType.T2_MICRO)
                .userData(user_data)
                .maxCount(1)
                .minCount(1).build();
        RunInstancesResponse response = null;
        try {
            response = ec2.runInstances(request);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Manger")
                .value("Manger")
                .build();

        CreateTagsRequest tag_request = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        try {
            ec2.createTags(tag_request);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return response.instances().get(0);
    }

    private static Instance get_instance_by_tag(Tag tag) {
        DescribeInstancesResponse reses = ec2.describeInstances();
        List<Reservation> rese_ls = reses.reservations();
        for (Reservation res : rese_ls) {
            List<Instance> instancelist = res.instances();
            for (Instance inst : instancelist) {
                if (inst.tags().contains(tag) & !(inst.state().name().equals(InstanceStateName.TERMINATED)))
                    return inst;
            }
        }
        return null;
    }
}

