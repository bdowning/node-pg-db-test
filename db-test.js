import { Database } from './database';
import sqla from 'sql-assassin';

const dbConfig = {
    host: process.env.PG_HOST || '/run/postgresql',
    user: process.env.PG_USER,
    database: process.env.PG_DB,
};

class MyDatabase extends Database {
    upsertKeyVal(pkey, val) {
        return this.query(sqla`
            INSERT INTO keyvals VALUES (${pkey}, ${val})
              ON CONFLICT (pkey) DO
            UPDATE keyvals SET val = ${val} WHERE pkey = ${pkey}
        `);
    }
};
