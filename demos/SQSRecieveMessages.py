import boto3
import time
 
# Wait for 5 seconds


sqs = boto3.resource('sqs')


def recieve():
	queue = sqs.get_queue_by_name(QueueName='test')
	while True:
		messages = queue.receive_messages(MessageAttributeNames=['Author'])
		if len(messages) == 0:
			print('No data')
		else:
			for message in messages:
			    # Get the custom author message attribute if it was set
			    author_text = ''
			    if message.message_attributes is not None:
			        author_name = message.message_attributes.get('Author').get('StringValue')
			        if author_name:
			            author_text = ' ({0})'.format(author_name)

			    # Print out the body and author (if set)
			    print('Hello, {0}!{1}'.format(message.body, author_text))
			    # Let the queue know that the message is processed
			    message.delete()

		time.sleep(2)


if __name__ == '__main__':
	recieve()
