import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2AsyncClient;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;


import javax.imageio.ImageIO;
import java.awt.Image;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class manger {
    private static Region region = Region.of("us-east-1");
    private static Ec2AsyncClient ec2 = Ec2AsyncClient.builder().region(region).build();
    private static S3AsyncClient s3 = S3AsyncClient.builder().region(region).build();
    private static SqsAsyncClient sqs = SqsAsyncClient.builder().region(region).build();
    private static String task="";
    private static boolean terminate = false;
    private static AtomicInteger task_counter = new AtomicInteger(0);
    private static Map<String, MessageAttributeValue> message_para= new HashMap<String,MessageAttributeValue>(); //sqs message format
    private static Semaphore Manger_sem = new Semaphore(1);
    private static String localapp_objects_name = "-manger";
    private static String manger_objects_name = "manger-manger";
    private static String worker_objects_name = "worker-manger";
    private static String image_id = "ami-024ea0d44cf7564b0"; // worker img
    private static Semaphore Thread_preprocess_sem = new Semaphore(1);
    private static Semaphore Thread_postprocess_sem = new Semaphore(1);
    private static Semaphore Thread_wait_for_workers_sem = new Semaphore(1);
    private static thread_run thread_obj = new thread_run();
    private static Map<String,Integer> thread_done_array = new HashMap<String,Integer>();
    private static Map<QueueAttributeName,String> queue_map = new HashMap<QueueAttributeName, String>();
    private static String user_data;
    private static String arn;


    public static void main(String[] args){
        try {
            init(args[0]);
            System.out.println("Manger_app_started");
            while (!terminate) {
                listen_queues();
                try {
                    Thread.currentThread().sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            check_for_any_open_tasks();
            clean_up();
        }catch (Exception e){System.out.println(e);}
    }

    private static void init(String arn_from_local_app) {
        queue_map.put(QueueAttributeName.fromValue("VisibilityTimeout"),"5");
        user_data = new String(Base64.getEncoder().encode((
                "#!/bin/bash -xe\n"+
                "exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1 \n"+
                "cd /home/ubuntu/worker\n"+
                "java -jar worker.jar"
        ).getBytes()));
        arn = arn_from_local_app;
    }

    private static void clean_up() {
        close_all_s3_buckets();
        close_all_sqs_queues();
        close_workers_instances();
        close_manger_instance();
    }

    private static void close_all_sqs_queues() {
        for(String url : sqs.listQueues().join().queueUrls()){
            delete_all_queue(url);
        }
    }

    private static void close_all_s3_buckets() {
        try{
            List<Bucket> ls = s3.listBuckets().join().buckets();
            for(Bucket bucket : ls){
                empty_bucket_by_name(bucket.name());
                delete_bucket_by_name(bucket.name());
            }
        }catch (S3Exception e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static void delete_all_queue(String url) {
        delete_queue_by_url(url);
    }

    private static void close_workers_instances() {
        terminate_instances_by_key("Worker");
    }

    private static void terminate_instances_by_key(String key) {
        DescribeInstancesResponse reses = ec2.describeInstances().join();
        List<Reservation> rese_ls = reses.reservations();
        for (Reservation res : rese_ls) {
            List<Instance> instancelist = res.instances();
            for (Instance inst : instancelist) {
                if(!(inst.state().name().equals(InstanceStateName.TERMINATED)))
                    for(Tag tag : inst.tags()){
                        if(tag.key().equals(key))
                        {
                            terminate_instance_by_id(inst.instanceId());
                        }
                    }
            }
        }
    }

    private static void close_manger_instance() {
        Tag tag = Tag.builder().key("Manger").value("Manger").build();
        Instance instance =get_instance_by_tag(tag);
        if(instance != null)
            terminate_instance_by_id(instance.instanceId());
        else{
            System.out.println("Instace id dosent exist"+ "manger,manger");
            System.exit(1);
        }
    }

    private static void stop_instance_by_id(String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder().instanceIds(instanceId).build();
        try{
            ec2.stopInstances(request).join();
        }catch(Ec2Exception e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static void terminate_instance_by_id(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder().instanceIds(instanceId).build();
        try{
            ec2.terminateInstances(request).join();
        }catch(Ec2Exception e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static int get_running_instance_by_key(String key) {
        int counter = 0;
        DescribeInstancesResponse reses = ec2.describeInstances().join();
        List<Reservation> rese_ls = reses.reservations();
        for (Reservation res : rese_ls) {
            List<Instance> instancelist = res.instances();
            for (Instance inst : instancelist) {
                for(Tag tag : inst.tags()){
                    if (tag.key().equals(key) & (inst.state().name().equals(InstanceStateName.RUNNING)))
                        counter++;
                }
            }
        }
        return counter;
    }

    private static Instance get_instance_by_tag(Tag tag) {
        DescribeInstancesResponse reses = ec2.describeInstances().join();
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

    private static void listen_queues(){
        List<String> queue_url_list = sqs.listQueues().join().queueUrls();
        check_queue_list(queue_url_list);
    }

    private static void check_queue_list(List<String> queue_url_list) {
        for(String queue_url : queue_url_list){
            if(!queue_url.contains("newtask") && !queue_url.contains("donetask"))
                if(check_queue_messages(queue_url))
                    break;
        }
    }

    private static boolean check_queue_messages(String queue_url){
        List<Message> messages;
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queue_url)
                .messageAttributeNames("All").maxNumberOfMessages(1).build();
        try {
            messages = sqs.receiveMessage(receiveMessageRequest).join().messages();
        }catch (Exception e) {
            return false;
        }
        if(messages.size() != 0) {
            Message message = messages.get(0);
            return check_for_manger_messages(message, queue_url);
        }
        return false;
    }

    private static boolean check_for_manger_messages(Message message, String queue_url) {
        MessageAttributeValue to_manger_value = MessageAttributeValue.builder().stringValue("manger").dataType("String").build();
        if(message.messageAttributes().get("to").equals(to_manger_value)) {
            change_message_visibility(message,queue_url);
            task = message.messageAttributes().get("action").stringValue();
            process_task(message,queue_url);
            delete_message_from_queue(message,queue_url);
            System.out.println("Manger seek for new messages");
            return true;
        }
        return false;
    }

    private static void Thread_delete_message_from_queue(Message message, String queue_url) {
        try{
            Manger_sem.acquire();
            DeleteMessageRequest request = DeleteMessageRequest.builder().queueUrl(queue_url).receiptHandle(message.receiptHandle()).build();
            sqs.deleteMessage(request);
        }catch (SqsException e){
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Manger_sem.release();
    }

    private static void delete_message_from_queue(Message message, String queue_url) {
        try{
            Manger_sem.acquire();
            DeleteMessageRequest request = DeleteMessageRequest.builder().queueUrl(queue_url).receiptHandle(message.receiptHandle()).build();
            sqs.deleteMessage(request);
        }catch (SqsException e){
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Manger_sem.release();
    }

    private static void process_task(Message message, String queue_url) {
        if(task.equals("stop")) {
            System.out.println("Manger got stop task");
            terminate = true;
        }
        else if(task.equals("start") & !terminate)
            process_start_task(message,queue_url);
        else if(task.equals("done")) {
            process_done_task(queue_url);
        }
        else{
            System.out.println("Manger got bad action message");
        }
    }

    private static void process_start_task(Message message, String queue_url) {
        System.out.println("Manger got new start message");
        task_counter.incrementAndGet();
        String id = get_id_from_queue_url_localapp_manger(queue_url);
        create_manger_manger_bucket(id);
        create_manger_manger_queue(id);
        upload_input_to_manger_s3(id);
        send_start_msg_to_manger_id(id,message);
        System.out.println("Manger create manger thread");
        create_manger_by_id(id);
    }

    private static void send_start_msg_to_manger_id(String id, Message message) {
        send_message("start","manger"+id,"manger",id,message.body());
    }

    private static void upload_input_to_manger_s3(String id) {
        String file_name = "input.txt";
        String bucket_name ="localapp"+id+ localapp_objects_name;
        File file = download_object_by_name(file_name,bucket_name);
        bucket_name =manger_objects_name+id;
        upload_object_by_name(file,bucket_name);
    }

    private static void create_manger_manger_queue(String id) {
        String queue_name = manger_objects_name+id;
        create_queue_by_name(queue_name);
    }

    private static String Thread_create_queue_by_name(String queue_name) {
        String queue_url=null;

        CreateQueueRequest request = CreateQueueRequest.builder().queueName(queue_name).attributes(queue_map).build();
        try{
            queue_url = sqs.createQueue(request).join().queueUrl();
        }catch (SqsException e){ System.out.println(e.awsErrorDetails().errorMessage()); System.exit(1);}
        return queue_url;
    }

    private static String create_queue_by_name(String queue_name) {
        String queue_url=null;
        CreateQueueRequest request = CreateQueueRequest.builder().queueName(queue_name).attributes(queue_map).build();
        try{
            queue_url = sqs.createQueue(request).join().queueUrl();
        }catch (SqsException e){ System.out.println(e.awsErrorDetails().errorMessage()); System.exit(1);}
        return queue_url;
    }

    private static void create_manger_manger_bucket(String id) {
        String bucket_name = manger_objects_name+id;
        create_bucket_by_name(bucket_name);
    }

    private static void create_bucket_by_name(String bucket_name) {
        CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket_name).build();
        try{
            s3.createBucket(request).join();
        }catch (S3Exception e){System.out.println(e.awsErrorDetails().errorMessage()); System.exit(1);}
    }

    private static void create_manger_by_id(String id) {
        Thread t = new Thread(thread_obj,id);
        t.start();
    }

    public static void thread_tasks() {
        try {
            System.out.println("Thread "+Thread.currentThread().getName()+" thread started");
            Thread_preprocess_sem.acquire();
            System.out.println("Thread "+Thread.currentThread().getName()+" aquired the first lock");
            System.out.println("Thread "+Thread.currentThread().getName()+" thread read number of workers");
            int num_of_workers = thread_read_numofworkers();
            System.out.println("Thread "+Thread.currentThread().getName()+" read input file");
            File file = read_input_file();
            check_file(file);
            System.out.println("Thread "+Thread.currentThread().getName()+" create aws objects");
            String[] queue_url_arr = create_queues_workers_manger();
            System.out.println("Thread "+Thread.currentThread().getName()+" split tasks");
            int num_of_tasks = split_tasks(file,queue_url_arr[0]);
            System.out.println("Thread "+Thread.currentThread().getName()+" check and create workers instances");
            check_and_create_workers(num_of_workers);
            Thread_preprocess_sem.release();
            System.out.println("Thread "+Thread.currentThread().getName()+" realeased first lock");
            System.out.println("Thread "+Thread.currentThread().getName()+" wait_for_workers");
            File summary = wait_workers(queue_url_arr[1], num_of_tasks);
            System.out.println("Thread "+Thread.currentThread().getName()+" aquire second lock");
            Thread_postprocess_sem.acquire();
            System.out.println("Thread "+Thread.currentThread().getName()+" upload result");
            upload_summary_to_s3(summary);
            System.out.println("Thread "+Thread.currentThread().getName()+" clean Queues");
            clean_workers(queue_url_arr);
            Thread_postprocess_sem.release();
            System.out.println("Thread "+Thread.currentThread().getName()+" released second lock");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread_postprocess_sem.release();
            Thread_preprocess_sem.release();
            thread_tasks();

        }
    }

    private static void clean_workers(String[] queue_url_arr) {
        for(String queue_url:queue_url_arr){
            thread_delete_queue_by_url(queue_url);
        }
    }

    private static int thread_read_numofworkers() {
        Message message =  thread_read_start_message();
        String id = Thread.currentThread().getName();
        Thread_delete_message_from_queue(message,get_queue_url_by_id(id,"manger","manger"+id));
        return Integer.parseInt(message.body());
    }

    private static Message thread_read_start_message() {
        String id = Thread.currentThread().getName();
        ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(get_queue_url_by_id(id,"manger","manger"+id))
                .attributeNamesWithStrings("All").build();
        try{
            List<Message> messages = sqs.receiveMessage(request).join().messages();
            for(Message message : messages){
                return message;
            }
        }catch (SqsException e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
        return null;
    }

    private static void upload_summary_to_s3(File file) {
        String bucket_name = manger_objects_name+Thread.currentThread().getName();
        Thread_upload_object_by_name(file,bucket_name);
        send_done_manger();
    }

    private static void send_done_manger() {
        System.out.println("Thread send done message");
        String action = "done";
        String id = Thread.currentThread().getName();
        String from = "manger"+id;
        String to = "manger";
        send_message(action,to,from,id);
    }


    private static File wait_workers(String queue_url, int num_of_tasks) {
        return listen_queue(queue_url,num_of_tasks);
    }

    private static File listen_queue(String queue_url, int num_of_tasks) {
        AtomicBoolean bool = new AtomicBoolean(false);
        List<Message> messages = null;
        File summary = null;
        thread_done_array.put(Thread.currentThread().getName(),0);
        while(!bool.get()){
            try{
                Thread_wait_for_workers_sem.acquire();
                bool.set(false);
                ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(queue_url)
                        .messageAttributeNames("All").waitTimeSeconds(3).build();
                messages = sqs.receiveMessage(request).join().messages();
                messages = Thread_check_error_message_from_worker(messages,queue_url);
                summary = process_messages(messages,queue_url);
                if(thread_done_array.get(Thread.currentThread().getName()) != null && num_of_tasks != thread_done_array.get(Thread.currentThread().getName()).intValue()) {
                    Thread_wait_for_workers_sem.release();
                    Thread.currentThread().sleep(1000);
                }
                else{
                    bool.set(true);
                    Thread_wait_for_workers_sem.release();
                }
            }catch(SqsException e){ System.out.println(e.awsErrorDetails().errorMessage()); System.exit(1);} catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return summary;
    }



    private static List<Message> Thread_check_error_message_from_worker(List<Message> messages, String queue_url) {
        MessageAttributeValue error_value = MessageAttributeValue.builder().stringValue("error").dataType("String").build();
        List<Message> return_ls = new LinkedList<>();
        for(Message message : messages){
            if(message.messageAttributes().get("action").equals(error_value)){
                error_in_worker(message.messageAttributes().get("worker_instance_id").stringValue(),message,queue_url);
            }
            else{
                return_ls.add(message);
            }
        }
        return return_ls;
    }

    private static void error_in_worker(String worker_instance_id, Message message, String queue_url) {
        System.out.println("Thread "+Thread.currentThread().getName()+" got error message from worker "+worker_instance_id);
        rebot_worker(worker_instance_id);
        System.out.println("Thread "+Thread.currentThread().getName()+" reboot worker "+worker_instance_id);
        Thread_delete_message_from_queue(message,queue_url);
    }

    private static void rebot_worker(String worker_instance_id) {
        RebootInstancesRequest request = RebootInstancesRequest.builder().instanceIds(worker_instance_id).build();
        try {
            ec2.rebootInstances(request).join();
        }catch (Ec2Exception e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static File process_messages(List<Message> messages,String queue_url) {
        String file_name = "summary" + Thread.currentThread().getName() + ".html";
        try {
            FileWriter fw = new FileWriter(file_name,true);
            String thread_id = Thread.currentThread().getName();
            for (Message message : messages) {
                fw.append(message.body());
                thread_done_array.put(thread_id,thread_done_array.get(thread_id)+1);
                delete_message_from_queue(message,queue_url);
            }
            fw.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new File(file_name);
    }

    private static void  check_and_create_workers(int num_of_workers) {
        if(num_of_workers == -1){
            System.out.println("Start message body not contain worker number");
            System.exit(1);
        }
        int running_workers = running_workers();
        if(!check_sufficent_running_workers(running_workers,num_of_workers))
            create_worker_instance(running_workers,num_of_workers);
    }

    private static void create_worker_instance(int running_workers, int num_of_workers) {
        for(int i=0; i < num_of_workers-running_workers;i++){
            if(i == 18)
                break;
            RunInstancesRequest request = RunInstancesRequest.builder()
                    .keyName("Manger_key")
                    .imageId(image_id)
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(1)
                    .minCount(1)
                    .userData(user_data)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(arn).build())
                    .build();
            Tag tag = Tag.builder().key("Worker").value("Worker"+Thread.currentThread().getName()).build();
            try{
                String inst = ec2.runInstances(request).join().instances().get(0).instanceId();
                Thread.currentThread().sleep(1000);
                CreateTagsRequest tag_request = CreateTagsRequest.builder().resources(inst).tags(tag).build();
                ec2.createTags(tag_request).join();
            }catch (Ec2Exception e){System.out.println(e.awsErrorDetails().errorMessage()); System.exit(1);} catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean  check_sufficent_running_workers(int running_workers, int num_of_workers) {
        if(running_workers < num_of_workers)
            return false;
        return true;
    }

    private static int running_workers() {
        return get_running_instance_by_key("Worker");
    }

    private static void check_file(File file) {
        if(file == null) {
            System.out.println("Could not find thread" + Thread.currentThread().getName() + " file");
            System.exit(1);
        }
    }

    private static int split_tasks(File file,String queue_url) {
        int counter =0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if(is_url_legal(line)) {
                    send_url_to_worker(line,queue_url);
                    counter++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return counter;
    }

    private static boolean is_url_legal(String image_url) {
        Image image = null;
        try {
            URL url = new URL(image_url);
            image = ImageIO.read(url);
        } catch (IOException e) {}
        if(image == null)
            return false;
        return true;
    }

    private static void send_url_to_worker(String line,String queue_url) {
        String id = Thread.currentThread().getName();
        String from = "manger"+id;
        String to = "worker";
        String action ="new image task";
        thread_send_message(action,to,from,id,line,queue_url);
    }

    private static String[] create_queues_workers_manger() {
        String[] returend_url = new String[2];
        String queue_name = worker_objects_name +Thread.currentThread().getName()+ "newtask";
        returend_url[0] = Thread_create_queue_by_name(queue_name);
        queue_name = worker_objects_name +Thread.currentThread().getName()+"donetask";
        returend_url[1] = Thread_create_queue_by_name(queue_name);
        return returend_url;
    }

    private static File read_input_file() {
        File file = thread_download_object_by_name("input.txt",manger_objects_name+Thread.currentThread().getName());
        return file;
    }

    private static void process_done_task(String queue_url) {
        System.out.println("Manger got new done message");
        String id = get_id_from_queue_url_manger_manger(queue_url);
        update_localapp(id);
        System.out.println("Manger clean up manger"+id);
        cleanup_manger(id);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        task_counter.decrementAndGet();
    }

    private static void cleanup_manger(String id) {
        delete_manger_id_queue(id);
        delete_manger_bucket(id);
    }

    private static void delete_manger_bucket(String id) {
        String bucket_name = manger_objects_name+id;
        empty_bucket_by_name(bucket_name);
        delete_bucket_by_name(bucket_name);
    }

    private static void empty_bucket_by_name(String bucket_name) {
        try {
            ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket_name).build();
            List<S3Object> ls = s3.listObjects(request).join().contents();
            for (S3Object s3Object : ls) {
                s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket_name)
                        .key(s3Object.key())
                        .build()).join();
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void delete_bucket_by_name(String bucket_name) {
        DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(bucket_name).build();
        try{
            s3.deleteBucket(request).join();
        }catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void delete_manger_id_queue(String id) {
        String queue_url = get_queue_url_by_id(id, "manger", "manger");
        if(queue_url != null)
            delete_queue_by_url(queue_url);
        else{
            System.out.println("Queue was not found"+"  manger"+id);
        }
    }

    private static void delete_queue_by_url(String queue_url) {
        DeleteQueueRequest request = DeleteQueueRequest.builder().queueUrl(queue_url).build();
        try{
            sqs.deleteQueue(request).join();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void thread_delete_queue_by_url(String queue_url) {
        DeleteQueueRequest request = DeleteQueueRequest.builder().queueUrl(queue_url).build();
        try{
            sqs.deleteQueue(request).join();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void update_localapp(String id) {
        System.out.println("Manger download and upload summary");
        download_upload_summary_to_s3(id);
        System.out.println("Manger send done message to localapp");
        send_done_localapp(id);
    }

    private static void send_done_localapp(String id) {
        String action = "done";
        send_message_to_localapp(id,action);
    }

    private static void send_message_to_localapp(String id, String action) {
        String from="manger";
        String to="localapp";
        send_message(action,to,from,id);
    }

    private static void thread_send_message(String action, String to,String from,String id,String body,String queue_url) {
        try {
            message_para.put("action",MessageAttributeValue.builder().stringValue(action).dataType("String").build());
            message_para.put("to",MessageAttributeValue.builder().stringValue(to).dataType("String").build());
            message_para.put("from",MessageAttributeValue.builder().stringValue(from).dataType("String").build());
            SendMessageRequest request = SendMessageRequest.builder().queueUrl(queue_url).messageBody(body).messageAttributes(message_para).build();
            sqs.sendMessage(request).join();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void send_message(String action, String to,String from,String id,String body) {
        try {
            Manger_sem.acquire();
            message_para.put("action",MessageAttributeValue.builder().stringValue(action).dataType("String").build());
            message_para.put("to",MessageAttributeValue.builder().stringValue(to).dataType("String").build());
            message_para.put("from",MessageAttributeValue.builder().stringValue(from).dataType("String").build());
            String queue_url = get_queue_url_by_id(id,from,to);
            if(queue_url != null) {
                SendMessageRequest request = SendMessageRequest.builder().queueUrl(queue_url).messageBody(body).messageAttributes(message_para).build();
                sqs.sendMessage(request).join();
            }else {
                System.out.println("Could not find queue");
            }
            Manger_sem.release();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void send_message(String action, String to,String from,String id) {
        try {
            Manger_sem.acquire();
            message_para.put("action",MessageAttributeValue.builder().stringValue(action).dataType("String").build());
            message_para.put("to",MessageAttributeValue.builder().stringValue(to).dataType("String").build());
            message_para.put("from",MessageAttributeValue.builder().stringValue(from).dataType("String").build());
            String queue_url = get_queue_url_by_id(id,from,to);
            if(queue_url != null) {
                SendMessageRequest request = SendMessageRequest.builder().queueUrl(queue_url).messageAttributes(message_para).messageBody(from+to).build();
                sqs.sendMessage(request).join();
            }else {
                System.out.println("Could not find queue");
            }
            Manger_sem.release();
        }catch(SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String get_queue_url_by_id(String id,String from,String to) {
        String queue_name= (from.equals("manger")& to.equals("localapp"))?"localapp"+id+"-manger":
                (from.equals("manger"+id)&to.equals("worker"))? worker_objects_name +id:"manger-manger"+id;
        GetQueueUrlRequest request = GetQueueUrlRequest.builder().queueName(queue_name).build();
        try{
            return sqs.getQueueUrl(request).join().queueUrl();
        }catch(SqsException e){System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
        return null;
    }

    private static void download_upload_summary_to_s3(String id) {
        File file = download_summary_from_manger_id(id);
        upload_summary_to_client_id(file,id);
    }

    private static void upload_summary_to_client_id(File file, String id) {
        String bucket_name = "localapp"+id+localapp_objects_name;
        upload_object_by_name(file,bucket_name);
    }

    private static void Thread_upload_object_by_name(File file,String bucket_name){
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket_name).key(file.getName()).build();
        try{
            s3.putObject(request, AsyncRequestBody.fromFile(file)).join();
        }catch (S3Exception e){ System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static void upload_object_by_name(File file,String bucket_name){
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket_name).key(file.getName()).build();
        try{
            s3.putObject(request, AsyncRequestBody.fromFile(file)).join();
        }catch (S3Exception e){ System.out.println(e.awsErrorDetails().errorMessage());System.exit(1);}
    }

    private static File download_summary_from_manger_id(String id) {
        String file_name = "summary" + id + ".html";
        String bucket_name = manger_objects_name + id;
        return download_object_by_name(file_name, bucket_name);
    }

    private static File thread_download_object_by_name(String file_name,String bucket_name){
        File file = new File("./", file_name);
        try {
            OutputStream os = new FileOutputStream(file);
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket_name).key(file_name).build();
            byte[] returned = s3.getObject(request, AsyncResponseTransformer.toBytes()).join().asByteArray();
            os.write(returned);
        } catch (S3Exception e) {
            e.awsErrorDetails().errorMessage();
            System.exit(1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    private static File download_object_by_name(String file_name,String bucket_name){
        File file = new File("./", file_name);
        try {
            OutputStream os = new FileOutputStream(file);
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket_name).key(file_name).build();
            byte[] returned = s3.getObject(request, AsyncResponseTransformer.toBytes()).join().asByteArray();
            os.write(returned);
        } catch (S3Exception e) {
            e.awsErrorDetails().errorMessage();
            System.exit(1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    private static String get_id_from_queue_url_manger_manger(String queue_url){
        return get_id_from_queue_url(queue_url,"manger-manger",queue_url.length());
    }

    private static String get_id_from_queue_url_localapp_manger(String queue_url){
        return get_id_from_queue_url(queue_url,"localapp",queue_url.lastIndexOf("-manger"));
    }

    private static String get_id_from_queue_url(String queue_url,String from,int till) {
        return queue_url.substring(queue_url.lastIndexOf('/')+1+from.length(),till);
    }

    private static void check_for_any_open_tasks() {
        while(task_counter.get() != 0){
            System.out.println("Manger have "+task_counter.get() + "open tasks");
            listen_queues();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            System.exit(1);
        }
    }


}
class thread_run implements Runnable {
    @Override
    public void run() {
        manger.thread_tasks();
    }
}


