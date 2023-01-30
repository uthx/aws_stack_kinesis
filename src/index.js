import axios from 'axios';
import { eventApiUrl } from './constants.js';
import { isErrorPresent } from './helper.js';
const handler = async (event) => {
    console.log("Handler is triggered")
    console.log(`Original_Event`, JSON.stringify(event));
    console.log(`Records_To_Process`, event.Records.length);
    
    try{
        for (const iterator of event.Records) { 
            let ESPayload = ``; 
            console.log(`Payload prior to parse`, iterator) 
            // ESPayload = Buffer.from(iterator.kinesis.data, `base64`).toString(`ascii`); 
            console.log(`ESPayload decoded`, ESPayload);
            if(isErrorPresent(ESPayload?.type)) {
                console.log("Skipping event processing, event", ESPayload)
            } else {
            console.log("eventApiUrl", eventApiUrl); 
            const resp = await axios.post(eventApiUrl, ESPayload); 
            console.log("resp status" , resp.status);
            console.log("resp" , resp.data); 
            //return resp.data 
            }
        }
        return "success";
    } catch(error){
        console.log("error", error)
        return error
    } 

};

export {handler};