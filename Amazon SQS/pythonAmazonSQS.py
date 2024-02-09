import boto3
import json
import logging

logging.basicConfig(level = logging.INFO)

logging.info("Connecting with AmazonSQS")

class AmazonSQSConnector:
	def __init__(self, region_name, aws_access_key_id, aws_secret_access_key):
		self.region_name = region_name
		self.aws_access_key_id = aws_access_key_id
		self.aws_secret_access_key = aws_secret_access_key
		self.sqs = boto3.client('sqs', region_name = self.region_name, aws_access_key_id = self.aws_access_key_id, aws_secret_access_key = self.aws_secret_access_key)
	
	def creatingQueue(self, createQueueDict):
		isQueueCreated = False
		responseMessage = ""
		try:
			if(len(createQueueDict) != 0):
				for k in createQueueDict.keys():
					queueValues = createQueueDict.get(k)
					logging.info(queueValues)
					if(queueValues != ""):
						queue = self.sqs.create_queue(QueueName = createQueueDict.get("QueueName"), Attributes = {"DelaySeconds": createQueueDict.get("DelaySeconds")})
						logging.info("Queue Created")
						isQueueCreated = True
						responseMessage = "Queue Created Successfully"
					else:
						responseMessage = "Provide valid queue values"
			else:
				responseMessage = "Provide all valid queue values"	
		except Exception as ex:
			logging.exception("Exception while creating Queue")
		return isQueueCreated, responseMessage
			
	def listingAllQueue(self):
		isQueueListed = False
		responseMessage = ""
		try:
			for queue in self.sqs.list_queues():
				logging.info("Queues Listed")
				logging.info(queue)
				isQueueListed =	True
				responseMessage = "Queue Listed Successfully"
		except Exception as ex:
			logging.exception("Exception while listing Queue")	
		return isQueueListed, responseMessage
			
	def sendingMessage(self, sendMessageDict):
		isMessageSent = False
		responseMessage = ""
		try:
			if(len(sendMessageDict) != 0):
				for k in sendMessageDict.keys():
					sentValues = sendMessageDict.get(k)
					logging.info(sentValues)
					if (sentValues != ""):
						getQueueUrl = self.sqs.get_queue_url(QueueName = sendMessageDict.get("QueueName"))
						queueUrl = getQueueUrl.get("QueueUrl")	
						logging.info(queueUrl)	
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
						logging.info(response.get('MessageId'))
						logging.info("\n\nMessage sent to Queue: %s"%(sendMessageDict.get("MessageBody")))
						logging.info("Message sent")
						isMessageSent = True
						responseMessage = "Message Sent Successfully"
					else:
						responseMessage = "Provide valid sent values"	
			else:
				responseMessage = "Provide all valid sent values"
		except Exception as ex:
			logging.exception("Exception while sending Message")	
		return isMessageSent, responseMessage
			
	def receivingMessage(self, receiveMessageDict):
		isMessageReceived = False
		responseMessage = ""
		try:
			if(len(receiveMessageDict) != 0):
				for k in receiveMessageDict.keys():
					receiveValues = receiveMessageDict.get(k)
					logging.info(receiveValues)
					if (receiveValues != ""):
						getQueueUrl = self.sqs.get_queue_url(QueueName = receiveMessageDict.get("QueueName"))
						queueUrl = getQueueUrl.get("QueueUrl")	
						logging.info(queueUrl)	
						response = self.sqs.receive_message(QueueUrl = queueUrl,
				    								AttributeNames = [receiveMessageDict.get("AttributeNames")],
				   								 	MaxNumberOfMessages = receiveMessageDict.get("MaxNumberOfMessages"),
				    								MessageAttributeNames = [receiveMessageDict.get("MessageAttributeNames")],
				    								VisibilityTimeout = receiveMessageDict.get("VisibilityTimeout"),
				    								WaitTimeSeconds = receiveMessageDict.get("WaitTimeSeconds"))
						logging.info("\n\nMessage received from Queue: %s"%(response))
						logging.info("Message received")
						isMessageReceived = True
						responseMessage = "Message Received Successfully"
					else:
						responseMessage = "Provide valid receive values"	
			else:
				responseMessage = "Provide all valid receive values"	
		except Exception as ex:
			logging.exception("Exception while receiving Message")		
		return isMessageReceived, responseMessage
			
	def deleteMessage(self, deleteMessageDict, receiptMessageDict):
		isMessageDeleted = False
		responseMessage = ""
		try:
			if(len(deleteMessageDict) != 0):
				if(len(receiptMessageDict) != 0):
					for j in deleteMessageDict.keys():
						deleteValues = deleteMessageDict.get(j)
						for k in receiptMessageDict.keys():
							receiptValues = receiptMessageDict.get(k)
							if (deleteValues != ""):
								if (receiptValues != ""):
									getQueueUrl = self.sqs.get_queue_url(QueueName = receiptMessageDict.get("QueueName"))
									queueUrl = getQueueUrl.get("QueueUrl")	
									logging.info(queueUrl)	
									response = self.sqs.receive_message(QueueUrl = queueUrl,
							    								AttributeNames = [receiptMessageDict.get("AttributeNames")],
							   								 	MaxNumberOfMessages = receiptMessageDict.get("MaxNumberOfMessages"),
							    								MessageAttributeNames = [receiptMessageDict.get("MessageAttributeNames")],
							    								VisibilityTimeout = receiptMessageDict.get("VisibilityTimeout"),
							    								WaitTimeSeconds = receiptMessageDict.get("WaitTimeSeconds"))
									messageJson = response["Messages"]
									logging.info(messageJson)
									for messageList in messageJson:
										logging.info(messageList)
										messageDict = messageList["ReceiptHandle"]
										logging.info(messageDict)
									getQueueUrl = self.sqs.get_queue_url(QueueName = deleteMessageDict.get("QueueName"))
									queueUrl = getQueueUrl.get("QueueUrl")	
									logging.info(queueUrl)	
									self.sqs.delete_message(QueueUrl = queueUrl,
							    					ReceiptHandle = messageDict)	
									logging.info("Message deleted")
									isMessageDeleted = True
									responseMessage = "Message Deleted Successfully"
								else:
									responseMessage = "Provide valid receipt values"
							else:
								responseMessage = "Provide valid de values"			
				else:
					responseMessage = "Provide all valid receipt values"
			else:
				responseMessage = "Provide all valid delete values"			
		except Exception as ex:
			logging.exception("Exception while deleting Message")			
		return isMessageDeleted, responseMessage
			
