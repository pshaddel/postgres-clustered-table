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
    // await insertDataToTables(normalProductTable, clustredProductTable, client);

    // setting the index
    await createIndex(normalProductTable, 'rating', client);
    await createIndex(clustredProductTable, 'rating', client);
    await setClusteredIndex(clustredProductTable, 'rating', client);


    console.log('Start Querying');

    await getProductsInRange(clustredProductTable, 2, 3, client);
    await getProductsInRange(normalProductTable, 2, 3, client);
    await getProductsInRange(clustredProductTable, 3, 4, client);
    await getProductsInRange(normalProductTable, 3, 4, client);
    await getProductsInRange(clustredProductTable, 4, 5, client);
    await getProductsInRange(normalProductTable, 4, 5, client);

};

main().then(() => process.exit(0)).catch((error) => { console.error(error); process.exit(1); });

/**
 * Table contains id, name, rating a decimal between 0 and 5
 */
async function createProductTable(tableName, client) {
    await client.query(`CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, name TEXT, rating DECIMAL(2, 1));`);
}

async function insertDataToTables(normalProductTable, clustredProductTable, client) {
    await createProductTable(normalProductTable, client);
    await createProductTable(clustredProductTable, client);


    let currrentInsert = []
    const rows = 200000;
    for (let i = 0; i < rows; i++) {
        const rating = Math.random() * 5;
        // batch each 1000 rows
        currrentInsert.push(`('product ${i}', ${rating})`);

        if (currrentInsert.length === 1000) {
            await client.query(`INSERT INTO ${normalProductTable} (name, rating) VALUES ${currrentInsert.join(',')};`);
            await client.query(`INSERT INTO ${clustredProductTable} (name, rating) VALUES ${currrentInsert.join(',')};`);
            currrentInsert = [];
            console.log(`Inserted ${i} rows - inserted percentage: ${i / rows * 100}%`);
        }
    }

    await client.end();
}

async function createIndex(tableName, columnName, client) {
    try {
        await client.query(`CREATE INDEX ${tableName}_${columnName}_index ON ${tableName} (${columnName});`);
    } catch (error) {
        console.log(`Index already exists on ${tableName} ${columnName}`);
    }
}

async function setClusteredIndex(tableName, columnName, client) {
    await client.query(`CLUSTER ${tableName} USING ${tableName}_${columnName}_index;`);
}


async function getProductsInRange(tableName, minRating, maxRating, client) {
    const uniqueName = `${tableName} ${minRating} - ${maxRating}`;
    console.time(uniqueName);
    const res = await client.query(`SELECT * FROM ${tableName} WHERE rating BETWEEN ${minRating} AND ${maxRating};`);
    console.timeEnd(uniqueName);
    const rowsLength = res.rows.length;
    console.log(`Found ${rowsLength} rows in range ${minRating} - ${maxRating}`);
}