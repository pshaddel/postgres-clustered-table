const pg = require('pg');
const main = async () => {
    const client = new pg.Client({
        user: 'user',
        host: 'localhost',
        database: 'db',
        password: 'pass',
        port: 5432,
    });

    await client.connect();

    const normalProductTable = 'normal_product';
    const clustredProductTable = 'clustred_product';

    // insert data to tables
    await insertDataToTables(normalProductTable, clustredProductTable, client);

    // get the size of the tables
    await getPostgresRecordSize(normalProductTable, client);
    await getPostgresRecordSize(clustredProductTable, client);
    await getPostgresSharedBufferSize(client);

    // setting the index
    await createIndex(normalProductTable, 'rating', client);
    await createIndex(clustredProductTable, 'rating', client);
    await setClusteredIndex(clustredProductTable, 'rating', client);
    await runAnalyze(client);


    console.log('Start Querying');
    // run range queries on both tables
    await runRangeQueriesOnBothTables(client, true);
    await runRangeQueriesOnBothTables(client, false);

    // run range queries on both tables with explain
    await runRangeQueriesOnBothTablesExplain(client, true);
    await runRangeQueriesOnBothTablesExplain(client, false);
};

main().then(() => process.exit(0)).catch((error) => { console.error(error); process.exit(1); });

/**
 * Table contains id, name, rating a decimal between 0 and 5, and a primary key id, add a field that we put large json object in it
 */
async function createProductTable(tableName, client) {
    await client.query(`DROP TABLE IF EXISTS ${tableName};`);
    await client.query(`CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, name TEXT, rating DECIMAL(2, 1), data JSONB);`);
    console.log(`Created table ${tableName}`);
}

async function insertDataToTables(normalProductTable, clustredProductTable, client) {
    await createProductTable(normalProductTable, client);
    await createProductTable(clustredProductTable, client);


    let currrentInsert = []
    const rows = 1000000;
    const batchSize = 3000;
    for (let i = 0; i < rows; i++) {
        const rating = Math.random() * 5;
        // a large json object
        const data = {
            name: `product ${i}`,
            rating: rating,
            description: Array(10000).fill('a').join(''),
        };
        // batch each 1000 rows
        currrentInsert.push(`('product ${i}', ${rating}, '${JSON.stringify(data)}')`);

        if (currrentInsert.length === batchSize) {
            await client.query(`INSERT INTO ${normalProductTable} (name, rating, data) VALUES ${currrentInsert.join(',')};`);
            await client.query(`INSERT INTO ${clustredProductTable} (name, rating, data) VALUES ${currrentInsert.join(',')};`);
            currrentInsert = [];
            console.log(`Inserted ${i} rows - inserted percentage: ${(i / rows * 100).toFixed(3)}%`);
        }
    }

}

async function createIndex(tableName, columnName, client) {
    try {
        console.time(`Index ${tableName} ${columnName}`);
        await client.query(`CREATE INDEX ${tableName}_${columnName}_index ON ${tableName} (${columnName});`);
        console.time(`Index ${tableName} ${columnName}`);
    } catch (error) {
        console.log(`Index already exists on ${tableName} ${columnName}`);
    }
}

async function setClusteredIndex(tableName, columnName, client) {
    console.time(`CLUSTER ${tableName} USING ${tableName}_${columnName}_index`);
    await client.query(`CLUSTER ${tableName} USING ${tableName}_${columnName}_index;`);
    console.timeEnd(`CLUSTER ${tableName} USING ${tableName}_${columnName}_index`);
}


async function getProductsInRange(tableName, minRating, maxRating, client) {
    const uniqueName = `${tableName} ${minRating} - ${maxRating}`;
    const time = Date.now();
    const res = await client.query(`SELECT * FROM ${tableName} WHERE rating BETWEEN ${minRating} AND ${maxRating};`);
    const timeDiff = Date.now() - time;
    const timeDiffMilli = (timeDiff / 1000).toFixed(3);
    const rowsLength = res.rows.length;
    const tableNameStringWith30Charachters = tableName.padEnd(30, ' ');
    console.log(`${tableNameStringWith30Charachters} [${minRating} - ${maxRating}] ${timeDiffMilli} ms`);
}

async function getProductsInRangeExplain(tableName, minRating, maxRating, client) {
    const res = await client.query(`EXPLAIN (analyze, buffers) SELECT * FROM ${tableName} WHERE rating BETWEEN ${minRating} AND ${maxRating};`);
    let buffersSharedRead = res.rows.find(row => row && row['QUERY PLAN'] ? row['QUERY PLAN'].includes('Buffers: shared read') : false) || {};
    buffersSharedRead = buffersSharedRead['QUERY PLAN'] || '';

    console.log(`${tableName} [${minRating} - ${maxRating}] ${buffersSharedRead}`);
}

async function getPostgresRecordSize(tableName, client) {
    const res = await client.query(`SELECT pg_size_pretty(pg_total_relation_size('${tableName}'));`);
    console.log(`Table ${tableName} size: ${res.rows[0].pg_size_pretty}`);
}

async function runRangeQueriesOnBothTables(client, tableOrder) {
    const rangeSize = 0.1;
    const ranges = [];
    for (let i = 0; i < 5; i++) {
        ranges.push([rangeSize * i, rangeSize * (i + 1)]);
    }
    for (const range of ranges) {
        if (tableOrder) {
            await getProductsInRange('normal_product', range[0], range[1], client);
            await getProductsInRange('clustred_product', range[0], range[1], client);
        } else {
            await getProductsInRange('clustred_product', range[0], range[1], client);
            await getProductsInRange('normal_product', range[0], range[1], client);
        }
    }
}

async function runRangeQueriesOnBothTablesExplain(client, tableOrder) {
    const rangeSize = 0.1;
    const ranges = [];
    for (let i = 0; i < 5; i++) {
        ranges.push([rangeSize * i, rangeSize * (i + 1)]);
    }
    for (const range of ranges) {
        if (tableOrder) {
            await getProductsInRangeExplain('normal_product', range[0], range[1], client);
            await getProductsInRangeExplain('clustred_product', range[0], range[1], client);
        } else {
            await getProductsInRangeExplain('clustred_product', range[0], range[1], client);
            await getProductsInRangeExplain('normal_product', range[0], range[1], client);
        }
    }
}

async function runAnalyze(client) {
    await client.query('ANALYZE;');
}

async function getPostgresSharedBufferSize(client) {
    const res = await client.query(`SHOW shared_buffers;`);
    console.log(`Shared buffer size: ${res.rows[0].shared_buffers}`);
}

async function setPostgresSharedBufferSize(client, size) {
    const result = await client.query(`SET shared_buffers TO '${size}';`);
    console.log(`Shared buffer size set to ${size}`);

}