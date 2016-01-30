import Promise from 'bluebird';
import pg from 'pg';
Promise.promisifyAll(pg);

import sqla from 'sql-assassin';

// const dbConfig = {
//     host: process.env.PG_HOST || '/run/postgresql',
//     user: process.env.PG_USER,
//     database: process.env.PG_DB,
// }

export class Database {
    constructor(opts) {
        this.pgConfig = opts.pgConfig;
    }

    async connect() {
        const [db, done] = await pg.connectAsync(this.pgConfig);
        this.db = db;
        this.done = done;
        this.transactionLevel = 0;
    }

    async run(body) {
        await this.connect();
        let ret;
        try {
            ret = await body(this);
        } finally {
            this.done();
        }
        return ret;
    }

    async withTransaction(body) {
        const { transactionLevel } = this;
        let ret;
        if (transactionLevel === 0)
            await this.query('BEGIN');
        else
            await this.query('SAVEPOINT sp');
        try {
            this.transactionLevel = transactionLevel + 1;
            ret = await body(this);
            if (transactionLevel === 0)
                await this.query('COMMIT');
            else
                await this.query('RELEASE SAVEPOINT sp');
        } catch (e) {
            if (transactionLevel === 0) {
                await this.query('ROLLBACK');
            } else {
                await this.query('ROLLBACK TO SAVEPOINT sp');
                await this.query('RELEASE SAVEPOINT sp');
            }
            throw e;
        } finally {
            this.transactionLevel = transactionLevel;
        }
        return ret;
    }

    async withCommittedTransaction(body) {
        if (this.transactionLevel !== 1)
            throw new Error('Can only withCommittedTransaction from top-level transaction');
        let ret;
        await this.query('COMMIT');
        this.done();
        this.db = null;
        this.done = null;
        try {
            ret = await body(this);
        } finally {
            await this.connect();
            await this.query('BEGIN');
            this.transactionLevel = 1;
        }
        return ret;
    }

    getQueryName(sql) {
        if (sql.length <= 63)
            return sql;
        let longNames = this.db.longNames;
        if (!longNames)
            longNames = this.db.longNames = { i: 0, map: { } };
        var shortName = longNames.map[sql];
        if (!shortName) {
            shortName = '__short_name_' + (++longNames.i) + '__';
            longNames.map[sql] = shortName;
        }
        return shortName;
    }

    query(sql, values) {
        if (sql instanceof sqla.types.SqlA) {
            values = sql.values(values);
            sql = sql.query();
        }
        return this.db.queryAsync({
            name: this.getQueryName(sql),
            text: sql,
            values: values
        });
    }
}
