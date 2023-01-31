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
      // ESPayload = Buffer.from(iterator.kinesis.data, `base64`).toString(`ascii`);
      console.log(`ESPayload decoded`, ESPayload);
      if (isErrorPresent(ESPayload?.type)) {
        console.log("Skipping event processing, event", ESPayload);
      } else {
        console.log("eventApiUrl", eventApiUrl);
        const resp = await axios.post(eventApiUrl, ESPayload);
        if (resp.status !== 200) {
          unhandeledRecords.Records.push(iterator.kinesis.data);
        }
        console.log("resp status", resp.status);
        console.log("resp", resp.data);
        //return resp.data
      }
    }
    console.low("Processing is completed");
    if(unhandeledRecords.Records.length) {
        console.log("Pushing records back in Kinesis");
        await kinesis.putRecords({
            StreamName: process.env.STREAM_NAME,
            PartitionKey: 'pk1',
            Data: Buffer.from(unhandeledRecords.Records)  
        }).promise();
        console.log("Pushed Records in Kinesis");
    }
  } catch (error) {
    console.log("error", error);
    return error;
  }
};

export { handler };
