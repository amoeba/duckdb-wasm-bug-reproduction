import { useEffect } from "react";

import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_next from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

async function runWorkflow() {
  console.log("runWorkflow called");

  const fileBuffer = await fetch("/orders_0.01.parquet").then(
    (res) => res.arrayBuffer() // <- returns ArrayBuffer
  );

  console.log("got ArrayBuffer", { fileBuffer });

  const MANUAL_BUNDLES = {
    mvp: {
      mainModule: duckdb_wasm,
      mainWorker: mvp_worker,
    },
    eh: {
      mainModule: duckdb_wasm_next,
      mainWorker: eh_worker,
    },
  };

  // Select a bundle based on browser checks
  const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);

  // Instantiate the asynchronus version of DuckDB-wasm
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  // Setup step: Clear all files from OPFS so we start with a fresh DB
  const opfsRoot = await navigator.storage.getDirectory();
  for await (const handle of opfsRoot.values()) {
    await opfsRoot.removeEntry(handle.name, {
      recursive: handle.kind === "directory",
    });
  }

  // Open the DB from OPFS
  await db.open({
    path: "opfs://mydb.db",
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
  });

  // and open a connection...
  let conn = await db.connect();

  // Step 1: Create an ArrayBuffer instance from the Parquet data. I use fetch
  // here but in your case you would want to use your File handle.
  const parquetBuffer = await fetch("/orders_0.01.parquet").then(
    (res) => res.arrayBuffer() // <- returns ArrayBuffer
  );

  // Step 2: Copy the data into OPFS
  const fileHandle = await opfsRoot.getFileHandle("orders.parquet", {
    create: true,
  });
  const writable = await fileHandle.createWritable();
  await writable.write(parquetBuffer);
  await writable.close();

  // Step 3: Register the file handle with DuckDB
  await db.registerFileHandle(
    "orders.parquet",
    null,
    duckdb.DuckDBDataProtocol.BROWSER_FSACCESS,
    true
  );

  // Step 4: Work with the file as normal
  await conn.send(
    `CREATE TABLE orders AS SELECT * FROM read_parquet('orders.parquet')`
  );
  await conn.send(`CHECKPOINT;`);
  const result1 = await conn.send(`SELECT * FROM orders;`);
  for await (const batch of result1) {
    console.log("got batch with ", batch.numRows, "rows");
  }

  // Closing everything
  await conn.close();
  await db.terminate();
  await worker.terminate();

  console.log("Finished!");
}

function App() {
  useEffect(() => {
    console.log("Hello!");
    runWorkflow();
  }, []);

  return (
    <>
      <div>Hello World</div>
    </>
  );
}

export default App;
