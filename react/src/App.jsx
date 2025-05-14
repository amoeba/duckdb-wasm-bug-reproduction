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

  await db.open({
    path: `opfs://${dbPath}`,
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
  });

  let conn = await db.connect();

  // Step 2: Copy the data into OPFS
  const opfsRoot = await navigator.storage.getDirectory();
  const fileHandle = await opfsRoot.getFileHandle(opfsFileName, {
    create: true,
  });
  const writable = await fileHandle.createWritable();
  await writable.write(fileBuffer);
  await writable.close();

  for await (let [name, handle] of opfsRoot.entries()) {
    console.log({ name, handle });
  }

  // Step 3: Register the file handle with DuckDB
  await db.registerFileHandle(
    opfsFileName,
    null,
    duckdb.DuckDBDataProtocol.BROWSER_FSACCESS,
    true
  );

  // Step 4: Work with the file as normal
  // let create_res = await conn.send(
  //   `CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${opfsFileName}')`
  // );

  await conn.send(`CREATE TABLE ${tableName} (x INTEGER);`);
  await conn.send(`CHECKPOINT;`);

  console.log("after CHECKPOINT");
  const result1 = await conn.send(`SELECT * FROM ${tableName};`);
  for await (const batch of result1) {
    console.log("got rows back:", batch.numRows);
  }

  // Closing everything
  await conn.close();
  await db.terminate();
  await worker.terminate();
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
