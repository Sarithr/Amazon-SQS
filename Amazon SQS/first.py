import boto3
import json
import logging

logging.basicConfig(level = logging.DEBUG)

logging.info("Connecting with AmazonSQS")
class AmazonSQSConnector:
	def __init__(self, region_name, aws_access_key_id, aws_secret_access_key):
		self.region_name = region_name
		self.aws_access_key_id = aws_access_key_id
		self.aws_secret_access_key = aws_secret_access_key
		self.sqs = boto3.client('sqs', region_name = self.region_name, aws_access_key_id = self.aws_access_key_id, aws_secret_access_key = self.aws_secret_access_key)
	
	def creatingQueue(self, queueDict):
		queue = self.sqs.create_queue(QueueName = createQueueDict.get("QueueName"), Attributes = {"DelaySeconds": queueDict.get("DelaySeconds")})
		logging.debug("Queue Created")

	def listingAllQueue(self):
		for queue in self.sqs.list_queues():
			logging.debug("Queues Listed")
			logging.debug(queue)	

	def sendingMessage(self, sendMessageDict):
		getQueueUrl = self.sqs.get_queue_url(QueueName = sendMessageDict.get("QueueName"))
		queueUrl = getQueueUrl.get("QueueUrl")	
		logging.debug(queueUrl)	
		response = self.sqs.send_message(QueueUrl = queueUrl,
    								DelaySeconds = sendMessageDict.get("DelaySeconds"),
    								MessageAttributes={
							        "Title" : {
							            "DataType": sendMessageDict["MessageAttributes"]["Title"]["DataType"],
							            'StringValue': sendMessageDict["MessageAttributes"]["Title"]["StringValue"]
							        },
							        "Author" : {
							            "DataType" : sendMessageDict["MessageAttributes"]["Author"]["DataType"],
							            "StringValue": sendMessageDict["MessageAttributes"]["Author"]["StringValue"]
							        },
							        "WeeksOn" : {
							            "DataType" : sendMessageDict["MessageAttributes"]["WeeksOn"]["DataType"],
							            "StringValue" : sendMessageDict["MessageAttributes"]["WeeksOn"]["StringValue"]
							        }
							    },
							    MessageBody=(sendMessageDict.get("MessageBody")))
		logging.debug(response.get('MessageId'))
		logging.debug("Message sent")

	def receivingMessage(self, receiveMessageDict):	
		getQueueUrl = self.sqs.get_queue_url(QueueName = sendMessageDict.get("QueueName"))
		queueUrl = getQueueUrl.get("QueueUrl")	
		logging.debug(queueUrl)	
		response = self.sqs.receive_message(QueueUrl = queueUrl,
    								AttributeNames = [receiveMessageDict.get("AttributeNames")],
   								 	MaxNumberOfMessages = receiveMessageDict.get("MaxNumberOfMessages"),
    								MessageAttributeNames = [receiveMessageDict.get("MessageAttributeNames")],
    								VisibilityTimeout = receiveMessageDict.get("VisibilityTimeout"),
    								WaitTimeSeconds = receiveMessageDict.get("WaitTimeSeconds"))
		logging.debug("Message received")

	def deleteMessage(self, deleteMessageDict):
		getQueueUrl = self.sqs.get_queue_url(QueueName = deleteMessageDict.get("QueueName"))
		queueUrl = getQueueUrl.get("QueueUrl")	
		logging.debug(queueUrl)	
		self.sqs.delete_message(QueueUrl = queueUrl,
    					ReceiptHandle = deleteMessageDict.get("ReceiptHandle"))	
		logging.debug("Message deleted")

createQueueDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : "5"}				
	
sendMessageDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : 10,
				"MessageAttributes" : {"Title" : {"DataType" : "String", "StringValue" : "The Whistler"},
									"Author" : {"DataType" : "String", "StringValue" : "John Grisham"},
									"WeeksOn" : {"DataType" : "Number", "StringValue" : "6"}},
				"MessageBody" : "Hello AmazonSQS"} 


receiveMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "All",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
							
deleteMessageDict = {"QueueName" : "TestQueue",
					"ReceiptHandle" : "receipt_handle" }

messageObject = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')
messageObject.creatingQueue(createQueueDict)		
messageObject.listingAllQueue()
messageObject.sendingMessage(sendMessageDict)
messageObject.receivingMessage(receiveMessageDict)
messageObject.deleteMessage(deleteMessageDict)