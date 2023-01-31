import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import { StartingPosition } from 'aws-cdk-lib/aws-lambda';
// import * as kinesis from '@aws-cdk/aws-kinesis';


export class KinesisLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'KinesisLambdaQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
    const stream = new kinesis.Stream(this, 'MyKinesisStream', {
      streamName: 'MyKinesisStream'
    })
    const lambdaFunction = new lambda.Function(this, 'Function', {
      code: lambda.Code.fromAsset('src'),
      handler: 'index.handler',
      functionName: 'KinesisMessageHandler',
      runtime: lambda.Runtime.NODEJS_14_X,
      environment: {
        STREAM_NAME: stream.streamName,
        STREAM_ARM: stream.streamArn
        
      }
    });

    const eventSource = new lambdaEventSources.KinesisEventSource(stream, {
      startingPosition: lambda.StartingPosition.TRIM_HORIZON,
    });

    lambdaFunction.addEventSource(eventSource);
    stream.grantWrite(lambdaFunction)
  }
}
