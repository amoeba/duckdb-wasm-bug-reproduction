import { useEffect, useState } from "react";
import { initDuckDB, runLoadAndQueryExample } from "./duckdb";

function App() {
  const [isDBInitialized, setDBInitialized] = useState(false);
  const [isExampleRun, setIsExampleRun] = useState(false);
  const [DBInstance, setDBInstance] = useState(null);

  useEffect(() => {
    console.log("isDBInitialized effect triggered");

    if (isDBInitialized) {
      console.log("DB is already initialized");
      return;
    }

    let db = initDuckDB();
    setDBInstance(db);
    setDBInitialized(true);

    if (isExampleRun) {
      console.log("Example has already been run. Not running.");
      return;
    }
    runLoadAndQueryExample(db);
    setIsExampleRun(true);
  }, [isDBInitialized, isExampleRun]);

  return <></>;
}

export default App;
