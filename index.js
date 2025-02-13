import fs from 'fs';
import os from 'os';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { fileURLToPath } from 'url';

import { connect, createDataItemSigner } from '@permaweb/aoconnect';

const AO_URL = 'http://192.168.1.11:10000'; // http://relay.ao-hb.xyz

const NUM_WORKERS = 10;
const NUM_PROCESSES = 10;
const REQUESTS_PER_WORKER = 100; // Each worker sends this amount of messages per process so message count will be (NUM_PROCESSES * REQUESTS_PER_WORKER)

async function spawn(deps, args) {
  const address = await fetch(AO_URL + '/~meta@1.0/info/address').then((res) => res.text());
  
  const tags = [
    { name: 'authority', value: 'tYRUqrx6zuFFiix3MoSBYSPP3nMzi5EKf-lVYDEQz8A' },
    { name: 'device', value: 'process@1.0' },
    { name: 'scheduler-device', value: 'scheduler@1.0' },
    { name: 'execution-device', value: 'compute-lite@1.0' },
    { name: 'scheduler-location', value: address },
    { name: 'scheduler', value: address },
  ];
  if (args.tags && args.tags.length > 0) args.tags.forEach((tag) => tags.push(tag));

  try {
    const processId = await deps.ao.spawn({
      module: args.module ?? 'Do_Uc2Sju_ffp6Ev0AnLVdPtot15rvMjP-a9VVaA5fM',
      scheduler: args.scheduler ?? 'yTVz5K0XU52lD9O0iHh42wgBGSXgsPcu7wEY8GqWnFY',
      signer: deps.signer,
      tags: tags,
      data: args.data ?? '1234',
    });

    console.log(`Spawned Process ID: ${processId}`);
    return processId;
  } catch (e) {
    throw new Error(e.message ?? 'Error spawning process');
  }
}

async function send(deps, args) {
  try {
    const tags = [{ name: 'Action', value: args.action }];
    if (args.tags) tags.push(...args.tags);

    const data = args.useRawData ? args.data : JSON.stringify(args.data);

    const txId = await deps.ao.message({
      process: args.processId,
      signer: deps.signer,
      tags: tags,
      data: data ?? '1234',
    });

    return txId;
  } catch (e) {
    throw new Error(e.message ?? 'Error sending message');
  }
}

async function runTest() {
    const { REQUESTS_PER_WORKER, processIds, wallet, AO_URL } = workerData;
  
    const deps = {
      ao: connect({ MODE: 'mainnet', AO_URL, wallet }),
      signer: createDataItemSigner(wallet)
    };
  
    let totalSuccess = 0;
    let totalFailure = 0;
  
    // Total messages will be (number of processes * messages per process)
    const totalMessages = processIds.length * REQUESTS_PER_WORKER;
    const promises = [];
  
    for (let i = 0; i < totalMessages; i++) {
      const randomProcessId = processIds[Math.floor(Math.random() * processIds.length)];
      promises.push(
        send(deps, { processId: randomProcessId, action: 'HB-Test' })
          .then((txId) => {
            totalSuccess++;
            console.log(`Worker ${process.pid}: Sent message to process ${randomProcessId} (tx: ${txId})`);
          })
          .catch((error) => {
            totalFailure++;
            console.error(`Worker ${process.pid}: Failed sending message to process ${randomProcessId}:`, error);
          })
      );
    }
  
    await Promise.all(promises);
    parentPort.postMessage({ success: totalSuccess, failure: totalFailure });
  }

if (isMainThread) {
  // Main thread: spawn processes and then create worker threads
  (async () => {
    try {
      const wallet = JSON.parse(fs.readFileSync(process.env.PATH_TO_WALLET, 'utf8'));
      
      const deps = {
        ao: connect({ MODE: 'mainnet', AO_URL, wallet }),
        signer: createDataItemSigner(wallet)
      };

      const processPromises = [];
      for (let i = 0; i < NUM_PROCESSES; i++) {
        processPromises.push(spawn(deps, {}));
      }
      const processIds = await Promise.all(processPromises);
      console.log(`All processes spawned: ${processIds}`);

      let completedWorkers = 0;
      let aggregatedSuccess = 0;
      let aggregatedFailure = 0;

      for (let i = 0; i < NUM_WORKERS; i++) {
        const worker = new Worker(fileURLToPath(import.meta.url), {
          workerData: {
            REQUESTS_PER_WORKER,
            processIds,
            wallet,
            AO_URL
          }
        });

        worker.on('message', ({ success, failure }) => {
          aggregatedSuccess += success;
          aggregatedFailure += failure;
        });

        worker.on('exit', () => {
          completedWorkers++;
          if (completedWorkers === NUM_WORKERS) {
            console.log(`All workers completed.`);
            console.log(`Total Successful Messages: ${aggregatedSuccess}`);
            console.log(`Total Failed Messages: ${aggregatedFailure}`);
          }
        });

        worker.on('error', (err) => console.error(`Worker error:`, err));
      }
    } catch (error) {
      console.error("Error in main thread:", error);
    }
  })();
} else {
  // Worker thread
  runTest().catch(err => {
    console.error("Error in worker:", err);
    parentPort.postMessage({ success: 0, failure: 0 });
  });
}