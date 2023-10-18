import * as duckdb from '@duckdb/duckdb-wasm'
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url'
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url'
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url'
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url'
import {tableFromArrays} from "apache-arrow";

class DuckBuilder {
    private _db: duckdb.AsyncDuckDB | undefined = undefined

    async getDB(): Promise<duckdb.AsyncDuckDB> {
        if (this._db) {
            const db = this._db
            return new Promise((resolve) => {
                resolve(db)
            })
        } else {
            const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
                mvp: {
                    mainModule: duckdb_wasm,
                    mainWorker: mvp_worker,
                },
                eh: {
                    mainModule: duckdb_wasm_eh,
                    mainWorker: eh_worker,
                },
            }
            // Select a bundle based on browser checks
            const bundle = await duckdb.selectBundle(MANUAL_BUNDLES)
            // Instantiate the asynchronous version of DuckDB-wasm
            const worker = new Worker(bundle.mainWorker!)
            const logger = new duckdb.ConsoleLogger()

            const db = new duckdb.AsyncDuckDB(logger, worker)
            await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
            this._db = db
            return db
        }
    }
}

const duckBuilder = new DuckBuilder()
const runTest = async () => {
    const db = await duckBuilder.getDB()
    const c = await db.connect()
    const duckName = `test_duck`

    const LENGTH = 2000;

    const rainAmounts = Float32Array.from(
        {length: LENGTH},
        () => Number((Math.random() * 20).toFixed(1)));

    const rainDates = Array.from(
        {length: LENGTH},
        (_, i) => new Date(Date.now() - 1000 * 60 * 60 * 24 * i));

    const rainfall = tableFromArrays({
        precipitation: rainAmounts,
        date: rainDates
    });
    console.log(rainfall.toString())

    try {
        await c.insertArrowTable(rainfall, {name: duckName})
        const t = (await c.query('SHOW ALL TABLES')).toString()
        console.log('SHOW TABLES RETURNED', t)
        //const t2 = (await c.query(`SELECT sum(precipitation) FROM ${duckName}`)).toString()
        //console.log('SELECT RETURNED', t2)
        document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
  <div>
    <h1>SHOW TABLES RETURNS</h1>
    <div>
    ${t}
</div>

  </div>`


    } finally {
        await c.close()
    }
}

runTest()
