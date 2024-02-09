import unittest, logging
from pythonAmazonSQS import AmazonSQSConnector
#import socket

class AmazonSQSTestCase(unittest.TestCase):
	def setup(self):
		logging.basicConfig(level = logging.DEBUG)
		#socket.getaddrinfo('localhost', 8080)

	def testCreatingQueue(self):
		createQueueDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : "5"}	
		createQueueData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')			
		isQueueCreated, responseMessage = createQueueData.creatingQueue(createQueueDict)		
		self.assertTrue(isQueueCreated)
		self.assertEquals(responseMessage, "Queue Created Successfully")

	def testListingQueue(self):
		listQueueData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')			
		isQueueListed, responseMessage = listQueueData.listingAllQueue()
		self.assertTrue(isQueueListed)
		self.assertEquals(responseMessage, "Queue Listed Successfully")	

	def testSendingMessage(self):
		sendMessageDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : 10,
				"MessageAttributes" : {"Title" : {"DataType" : "String", "StringValue" : "The Whistler"},
									"Author" : {"DataType" : "String", "StringValue" : "John Grisham"},
									"WeeksOn" : {"DataType" : "Number", "StringValue" : "6"}},
				"MessageBody" : "Hello AmazonSQS"} 
		sendMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageSent, responseMessage = sendMessageData.sendingMessage(sendMessageDict)
		self.assertTrue(isMessageSent)
		self.assertEquals(responseMessage, "Message Sent Successfully")

	def testReceivingMessage(self):
		receiveMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "All",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		receiveMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageReceived, responseMessage = receiveMessageData.receivingMessage(receiveMessageDict)
		self.assertTrue(isMessageReceived)
		self.assertEquals(responseMessage, "Message Received Successfully")

	def testDeletingMessage(self):
		receiptMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "All",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		deleteMessageDict = {"QueueName" : "TestQueue",
					"ReceiptHandle" : "receipt_handle" }
		deleteMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageDeleted, responseMessage = deleteMessageData.deleteMessage(deleteMessageDict, receiptMessageDict)
		self.assertTrue(isMessageDeleted)
		self.assertEquals(responseMessage, "Message Deleted Successfully")

	def testCreatingQueueWithoutData(self):
		createQueueDict = {}
		createQueueData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')			
		isQueueCreated, responseMessage = createQueueData.creatingQueue(createQueueDict)		
		self.assertFalse(isQueueCreated)
		self.assertEquals(responseMessage, "Provide all valid queue values")	

	def testSendingMessageWithoutData(self):
		sendMessageDict = {}
		sendMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageSent, responseMessage = sendMessageData.sendingMessage(sendMessageDict)
		self.assertFalse(isMessageSent)
		self.assertEquals(responseMessage, "Provide all valid sent values")

	def testReceivingMessageWithoutData(self):
		receiveMessageDict = {}
		receiveMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageReceived, responseMessage = receiveMessageData.receivingMessage(receiveMessageDict)
		self.assertFalse(isMessageReceived)
		self.assertEquals(responseMessage, "Provide all valid receive values")

	def testDeletingMessageWithoutReceiptData(self):
		receiptMessageDict = {}
		deleteMessageDict = {"QueueName" : "TestQueue",
					"ReceiptHandle" : "receipt_handle" }
		deleteMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageDeleted, responseMessage = deleteMessageData.deleteMessage(deleteMessageDict, receiptMessageDict)
		self.assertFalse(isMessageDeleted)
		self.assertEquals(responseMessage, "Provide all valid receipt values")

	def testDeletingMessageWithoutDeleteData(self):
		receiptMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "All",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		deleteMessageDict = {}
		deleteMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageDeleted, responseMessage = deleteMessageData.deleteMessage(deleteMessageDict, receiptMessageDict)
		self.assertFalse(isMessageDeleted)
		self.assertEquals(responseMessage, "Provide all valid delete values")	

	def testCreatingQueueWithoutValues(self):
		createQueueDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : ""}	
		createQueueData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')			
		isQueueCreated, responseMessage = createQueueData.creatingQueue(createQueueDict)		
		self.assertFalse(isQueueCreated)
		self.assertEquals(responseMessage, "Provide valid queue values")
					
	def testSendingMessageWithoutValues(self):
		sendMessageDict = {"QueueName" : "TestQueue",
				"DelaySeconds" : 10,
				"MessageAttributes" : {"Title" : {"DataType" : "String", "StringValue" : "The Whistler"},
									"Author" : {"DataType" : "String", "StringValue" : "John Grisham"},
									"WeeksOn" : {"DataType" : "Number", "StringValue" : "6"}},
				"MessageBody" : ""}
		sendMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageSent, responseMessage = sendMessageData.sendingMessage(sendMessageDict)
		self.assertFalse(isMessageSent)
		self.assertEquals(responseMessage, "Provide valid sent values")		 					

	def testReceivingMessageWithoutValues(self):
		receiveMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		receiveMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageReceived, responseMessage = receiveMessageData.receivingMessage(receiveMessageDict)
		self.assertFalse(isMessageReceived)
		self.assertEquals(responseMessage, "Provide valid receive values")

	def testDeletingMessageWithoutReceiptValues(self):
		receiptMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		deleteMessageDict = {"QueueName" : "TestQueue",
					"ReceiptHandle" : "receipt_handle" }				
		deleteMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageDeleted, responseMessage = deleteMessageData.deleteMessage(deleteMessageDict, receiptMessageDict)
		self.assertFalse(isMessageDeleted)
		self.assertEquals(responseMessage, "Provide valid receipt values")			

	def testDeletingMessageWithoutDeleteValues(self):
		receiptMessageDict = {"QueueName" : "TestQueue",
					"AttributeNames" : "SentTimestamp",
					"MaxNumberOfMessages" : 1,
					"MessageAttributeNames" : "",
					"VisibilityTimeout" : 0,
					"WaitTimeSeconds" : 0}	
		deleteMessageDict = {"QueueName" : "TestQueue",
					"ReceiptHandle" : "" }				
		deleteMessageData = AmazonSQSConnector('ap-south-1', 'AKIATMGVFWAEJBQPIDEY', '6zQQdkf5a466mREJtQoc8QhGXStF2aEotq7IAVmx')				
		isMessageDeleted, responseMessage = deleteMessageData.deleteMessage(deleteMessageDict, receiptMessageDict)
		self.assertFalse(isMessageDeleted)
		self.assertEquals(responseMessage, "Provide valid delete values")	