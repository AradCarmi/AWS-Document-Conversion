
#The project
In our project we implement image OCR system that work in distributed methods.
The system support multiply local apps with one manger that support multi threads.

‫#‬Flow chart
For each local-app the main thread of the manger will open new thread which the thread, will take care of‫:‬ create workers‫,‬ split the image url to tasks‫,‬ summary all responses from the workers and send the main thread of the manger that the summary is ready.The main thread of the manger is also responsible to update the local-apps when the file is in s3 and in the end when stop task is received, the main thread clean up all the objects and open Ec2 instances.   


#Scalability
The program supports high scalability which implemented in manger program. We implemented the manger in a way of having only one client of each Aws objects that work with asyncs methods. Each image url is sent by specsefic a queue to the workers poll, and the response is process by thread in the manger program step by step which make it more scalable.


#Security
The credentials are saved in local document in the local computer which make it safe to upload and send the projects jars.The Iam role ARN, which permit each instance that have it to do several action, saved in the local env of the project and convey in the user-data command args when the manger instance is startup.

#Aws objects
Each local-app is supplied with unique id that will define each Aws objects name. For transfer data between the local app and the manger, one will have a unique s3 bucket and sqs queue. The manger thread also created by the local app id. The manger thread will open two queues to the workers.The first one is only for sending new OCR tasks while the other one is used for Done tasks.

#Error Handling
Each worker that encounter with error will retry get another task and also send error message to the manger which will reboot the instance. Because the task queue is not deliver for particular worker, the message will be processed by another worker
  