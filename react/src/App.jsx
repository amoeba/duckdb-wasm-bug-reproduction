import { useEffect, useState } from "react";
import { initDuckDB, handleFile } from "./duckdb";

async function handleChange(e, DBPromise) {
  console.log("handleChange called");
  let file = e.target.files[0];
  await handleFile(file, DBPromise);
  console.log("handleChange finished!");
}

function App() {
  const [isDBInitialized, setDBInitialized] = useState(false);
  const [DBInstance, setDBInstance] = useState(null);

  useEffect(() => {
    console.log("useEffect triggered");

    if (isDBInitialized) {
      console.log("DB is already initialized");
      return;
    }

    let db = initDuckDB();
    setDBInstance(db);
    setDBInitialized(true);
  }, [isDBInitialized]);

  return (
    <>
      <input
        type="file"
        onChange={(e) => {
          handleChange(e, DBInstance);
        }}
      />
    </>
  );
}

export default App;
