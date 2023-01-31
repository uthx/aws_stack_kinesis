import axios from "axios";
import { eventApiUrl } from "./constants.js";
import { isErrorPresent } from "./helper.js";
import AWS from 'aws-sdk';

const kinesis = new AWS.Kinesis({apiVersion: '2013-12-02'});
const handler = async (event) => {
  try {
    console.log("Handler is triggered");
    console.log(`Original_Event`, JSON.stringify(event));
    console.log(`Records_To_Process`, event.Records.length);
    const unhandeledRecords = { Records: [] };
    for (const iterator of event.Records) {
      let ESPayload = ``;
      console.log(`Payload prior to parse`, iterator);
      ESPayload = Buffer.from(iterator.kinesis.data, `base64`).toString(`ascii`);
      console.log(`ESPayload decoded`, ESPayload);
      if (isErrorPresent(ESPayload?.type)) {
        console.log("Skipping event processing, event", ESPayload);
      } else {
        console.log("eventApiUrl", eventApiUrl);
        const resp = await axios.post(eventApiUrl, ESPayload);
        resp.status = 400;
        if (resp.status !== 200) {
          unhandeledRecords.Records.push(iterator.kinesis.data);
        }
        console.log("resp status", resp.status);
        console.log("resp", resp.data);
        //return resp.data
      }
    }
    console.log("Processing is completed");
    console.log({
      unhandeledRecords,
      "streamName": process.env.STREAM_NAME,
      "streamArn" : process.env.STREAM_ARN
    })
    
    if(unhandeledRecords.Records.length) {
        console.log("Pushing records back in Kinesis");
        await kinesis.putRecords({
            StreamName: process.env.STREAM_NAME,
            StreamARN: process.env.STREAM_ARN,
            Records: unhandeledRecords.Records
        }).promise();
        console.log("Pushed Records in Kinesis");
    }
  } catch (error) {
    console.log("error", error);
    return error;
  }
};

export { handler };
