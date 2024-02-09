import boto3
sqs = boto3.resource("sqs", region_name = "ap-south-1", aws_access_key_id = "AKIATMGVFWAEJBQPIDEY", aws_secret_access_key = "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")

queue = sqs.create_queue(QueueName = 'TestQueue', Attributes = {'DelaySeconds': '5'})

print(queue.url)
print(queue.attributes.get('DelaySeconds'))