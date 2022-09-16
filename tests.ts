var AWS =require('aws-sdk');

AWS.config.update({ region: process.env['AWS_REGION'],
                accessKeyId: process.env['AWS_ACCESS_KEY_ID'],
                secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY']})


const sqs = new AWS.SQS();
const queueURL = process.env['AWS_SQS__QUEUE_URL'];
                
const sns = new AWS.SNS();
const snsTopicARN = process.env['AWS_SNS_TOPIC_ARC'];


const lambda = new AWS.Lambda();

async function pollFromSQS ()  {
console.log('Started SQS polling.');
                  var params = {
                      AttributeNames: [
                          "SentTimestamp"
                      ],
                      MaxNumberOfMessages: 10,
                      MessageAttributeNames: [
                          "All"
                      ],
                      QueueUrl: queueURL,
                      WaitTimeSeconds: 20
                  };                
                
                  const result = await sqs.receiveMessage(params).promise().catch(() => console.log('Receiving message failed.'))
                  
                   return result;
                }
            
              
describe('Integration tests',() => {

test('Publish sns message and expect message to reach SQS ', async () => {
   
    let sns = new AWS.SNS();
    var params = {
        Message: 'Test lambda invocation from Auto tests',
        TopicArn: snsTopicARN
    };
   await sns.publish(params).promise()
   .then(() => console.log('message sent successfully.'))
   .catch(() => console.log('Something went wrong while sending SNS message to sqs queue.'));
   const result =   await pollFromSQS();
   if(result && result.Messages && result.Messages.length > 0) {
    const body = JSON.parse(result.Messages[0].Body);
    console.log(body)
  expect(JSON.stringify(body)).toContain('Test lambda invocation from Auto tests')
   }
  });

test('invoke lamda with SQS Event',  async() => {
  const input = {
  Records: [
    {
      messageId: "d90fd5a5-fd9e-4ab0-979b-97a1e70c9587",
      receiptHandle: "AQEB3Z4KHgpG7c/PG+QzcQ8+lfkZTtoS902r67GNes0Oo4JvcaEzkpTYoUzWTtbkhwbrJcxX36YvNW73oJXiNRnZjKHMkv9348JwBfLc9ES32IrK7w2RTXJ+Odl1mMIJCnuYGaiM61HxymbBRn3MmDHiOHqPytTwYSUNsZWP+OZRWncmPTBjyqrdq1/bItRLAtIM02WR6r3S+YyjCYLO0kKlYs0g4JZAEJ7CD8VXvDJnuDTBFPGv+5a9HaJRsxwF1LdksC5YYdEQ7uScKHm0gZFGLHyifN6S2J3x6vzooSR72gmUx1Bu43U3yu2arbwbykaO+40NjfsxK/Z43cXStWIlV+V7ZX5kJ9YTpqkOKujZtmZ4fYZXcns/WYEiwuw9eoPFaSMdVJyFCPScNlsGvfcHc8IkjXC0TbhV68XJYb7eR6Y=",
      body: "Hi there",
      attributes: {
        ApproximateReceiveCount: "1",
        SentTimestamp: "1602074535529",
        SenderId: "123456789012",
        ApproximateFirstReceiveTimestamp: "1602074535540"
      },
      messageAttributes: {},
      md5OfBody: "033bd94b1168d7e4f0d644c3c95e35bf",
      eventSource: "aws:sqs",
      eventSourceARN: process.env['AWS_QUEUE_ARN'],
      awsRegion: "eu-west-2"
    }
  ]
}
  

const params = {
  FunctionName: 'test',
  InvocationType: 'RequestResponse',
  LogType: 'Tail',
  Payload: JSON.stringify(input),
}
console.log("Invoked Lambda...")
const data = await lambda.invoke(params).promise() 
console.log('Data: ', data) 
console.log("Lambda done")

expect((data.StatusCode)).toBe(200);
expect(JSON.stringify(data)).toContain('Hello from Lambda!')

});
});

 



            
    
       




