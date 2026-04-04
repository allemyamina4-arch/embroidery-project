const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');
const crypto = require('crypto');

// ═══════════════════════════════════════════════════════
// ★★★ دالة توليد UUID آمنة ★★★
// ═══════════════════════════════════════════════════════
let uuidLib = null;
try {
    uuidLib = require('uuid');
} catch (e) {
    // سنستخدم crypto كبديل
}

function generateUUID() {
    // المحاولة 1: مكتبة uuid
    if (uuidLib && typeof uuidLib.v4 === 'function') {
        return uuidLib.v4();
    }
    // المحاولة 2: crypto.randomUUID (Node 19+)
    if (crypto && typeof crypto.randomUUID === 'function') {
        return crypto.randomUUID();
    }
    // المحاولة 3: توليد يدوي
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['polling', 'websocket'],
    pingTimeout: 120000,
    pingInterval: 25000,
    maxHttpBufferSize: 5e6
});

// ═══════════════════════════════════════════════════════
// ★★★ إعداد PostgreSQL ★★★
// ═══════════════════════════════════════════════════════
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

pool.on('error', (err) => {
    console.error('⚠️ خطأ PostgreSQL:', err.message);
});

// ═══════════════════════════════════════════════════════
// ★★★ V5: إنشاء الجداول + Migration تلقائي ★★★
// ═══════════════════════════════════════════════════════
async function initDatabase() {
    const client = await pool.connect();
    try {
        // ─── الخطوة 1: إنشاء الجداول إذا لم تكن موجودة ───
        await client.query(`
            CREATE TABLE IF NOT EXISTS message_buffer (
                id SERIAL PRIMARY KEY,
                seq INTEGER UNIQUE NOT NULL,
                uuid VARCHAR(36),
                type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                hash VARCHAR(64),
                sender VARCHAR(100),
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                acked_by TEXT[] DEFAULT '{}'
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS machine_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                status VARCHAR(50) DEFAULT 'UNKNOWN',
                speed REAL DEFAULT 0,
                last_data JSONB DEFAULT '{"tailor":"---","color":"---","ficha_id":"---"}',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                current_seq INTEGER DEFAULT 0
            )
        `);

        // ─── الخطوة 2: ★★★ Migration — إضافة الأعمدة المفقودة ★★★ ───
        console.log('🔄 جاري فحص أعمدة الجدول...');

        // فحص وإضافة عمود uuid
        await safeAddColumn(client, 'message_buffer', 'uuid', 'VARCHAR(36)');
        // فحص وإضافة عمود hash
        await safeAddColumn(client, 'message_buffer', 'hash', 'VARCHAR(64)');
        // فحص وإضافة عمود sender
        await safeAddColumn(client, 'message_buffer', 'sender', 'VARCHAR(100)');
        // فحص وإضافة عمود acked_by
        await safeAddColumn(client, 'message_buffer', 'acked_by', "TEXT[] DEFAULT '{}'");
        // فحص وإضافة عمود timestamp
        await safeAddColumn(client, 'message_buffer', 'timestamp', 'TIMESTAMPTZ DEFAULT NOW()');

        // ─── الخطوة 3: ملء UUID للصفوف القديمة التي ليس لها uuid ───
        const nullUuids = await client.query(
            `SELECT id FROM message_buffer WHERE uuid IS NULL`
        );
        if (nullUuids.rows.length > 0) {
            console.log(`🔧 ملء ${nullUuids.rows.length} صف بدون UUID...`);
            for (const row of nullUuids.rows) {
                await client.query(
                    `UPDATE message_buffer SET uuid = $1 WHERE id = $2`,
                    [generateUUID(), row.id]
                );
            }
            console.log(`✅ تم ملء UUID للصفوف القديمة`);
        }

        // ─── الخطوة 4: ملء hash للصفوف القديمة ───
        const nullHashes = await client.query(
            `SELECT id, data FROM message_buffer WHERE hash IS NULL`
        );
        if (nullHashes.rows.length > 0) {
            console.log(`🔧 ملء ${nullHashes.rows.length} صف بدون Hash...`);
            for (const row of nullHashes.rows) {
                const hash = computeHash(
                    typeof row.data === 'string' ? JSON.parse(row.data) : row.data
                );
                await client.query(
                    `UPDATE message_buffer SET hash = $1 WHERE id = $2`,
                    [hash, row.id]
                );
            }
            console.log(`✅ تم ملء Hash للصفوف القديمة`);
        }

        // ─── الخطوة 5: إضافة UNIQUE constraint على uuid إذا لم يكن موجوداً ───
        try {
            await client.query(`
                CREATE UNIQUE INDEX IF NOT EXISTS idx_buffer_uuid_unique
                ON message_buffer(uuid) WHERE uuid IS NOT NULL
            `);
        } catch (e) {
            // قد يفشل إذا كانت هناك تكرارات — نتجاهل
            console.log(`⚠️ لم يتم إنشاء UNIQUE index على uuid: ${e.message}`);
        }

        // ─── الخطوة 6: فهارس ───
        await safeCreateIndex(client, 'idx_buffer_seq', 'message_buffer', 'seq');
        await safeCreateIndex(client, 'idx_buffer_uuid', 'message_buffer', 'uuid');
        await safeCreateIndex(client, 'idx_buffer_timestamp', 'message_buffer', 'timestamp');
        await safeCreateIndex(client, 'idx_buffer_type', 'message_buffer', 'type');

        // ─── الخطوة 7: بيانات ابتدائية ───
        await client.query(`
            INSERT INTO machine_state (id, status, last_data)
            VALUES (1, 'UNKNOWN', '{"tailor":"---","color":"---","ficha_id":"---"}')
            ON CONFLICT (id) DO NOTHING
        `);
        await client.query(`
            INSERT INTO sequence_counter (id, current_seq) VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        `);

        // ─── الخطوة 8: تقرير نهائي ───
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');

        // فحص أعمدة الجدول النهائية
        const columnsResult = await client.query(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'message_buffer' 
            ORDER BY ordinal_position
        `);
        console.log('═'.repeat(60));
        console.log('✅ قاعدة البيانات PostgreSQL جاهزة');
        console.log(`   📦 الرسائل المخزنة: ${countResult.rows[0].count}`);
        console.log(`   🔢 آخر SEQ: ${seqResult.rows[0]?.current_seq || 0}`);
        console.log(`   📋 أعمدة message_buffer:`);
        columnsResult.rows.forEach(col => {
            console.log(`      - ${col.column_name} (${col.data_type})`);
        });
        console.log('═'.repeat(60));

    } catch (err) {
        console.error('❌ خطأ إنشاء DB:', err.message);
        console.error('   Stack:', err.stack);
    } finally {
        client.release();
    }
}

// ═══════════════════════════════════════════════════════
// ★★★ دوال Migration المساعدة ★★★
// ═══════════════════════════════════════════════════════
async function safeAddColumn(client, table, column, definition) {
    try {
        // فحص هل العمود موجود
        const result = await client.query(`
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = $1 AND column_name = $2
        `, [table, column]);

        if (result.rows.length === 0) {
            // العمود غير موجود — أضفه
            await client.query(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
            console.log(`   ✅ تمت إضافة عمود: ${table}.${column} (${definition})`);
        } else {
            console.log(`   ✔️ العمود موجود: ${table}.${column}`);
        }
    } catch (err) {
        // إذا كان العمود موجوداً بالفعل (race condition)
        if (err.message.includes('already exists')) {
            console.log(`   ✔️ العمود موجود: ${table}.${column}`);
        } else {
            console.error(`   ❌ فشل إضافة ${table}.${column}: ${err.message}`);
        }
    }
}

async function safeCreateIndex(client, indexName, table, column) {
    try {
        await client.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${table}(${column})`);
    } catch (err) {
        // تجاهل إذا كان موجوداً
    }
}

// ═══════════════════════════════════════════════════════
// ★★★ دوال قاعدة البيانات ★★★
// ═══════════════════════════════════════════════════════
let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', color: '---', ficha_id: '---' };
let sequenceCounter = 0;
let dbAvailable = false;

async function loadStateFromDB() {
    try {
        const stateResult = await pool.query('SELECT * FROM machine_state WHERE id=1');
        if (stateResult.rows.length > 0) {
            machineStatus = stateResult.rows[0].status || 'UNKNOWN';
            lastData = stateResult.rows[0].last_data || lastData;
        }
        const seqResult = await pool.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        if (seqResult.rows.length > 0) {
            sequenceCounter = seqResult.rows[0].current_seq || 0;
        }
        dbAvailable = true;
        console.log(`✅ تحميل الحالة: status=${machineStatus}, seq=${sequenceCounter}`);
    } catch (err) {
        console.error('⚠️ فشل تحميل الحالة:', err.message);
        dbAvailable = false;
    }
}

// ★ حساب Hash
function computeHash(data) {
    try {
        const str = JSON.stringify(data, Object.keys(data).sort());
        return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
    } catch (e) {
        // fallback إذا فشل الترتيب
        const str = JSON.stringify(data);
        return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
    }
}

// ★ رقم تسلسلي جديد
async function getNextSeq() {
    sequenceCounter++;
    if (dbAvailable) {
        try {
            await pool.query('UPDATE sequence_counter SET current_seq=$1 WHERE id=1', [sequenceCounter]);
        } catch (err) {
            console.error('⚠️ فشل تحديث SEQ:', err.message);
        }
    }
    return sequenceCounter;
}

// ★ إضافة رسالة — مع UUID + Hash + حماية كاملة
async function addToBuffer(type, data, senderSocketId, clientUuid, clientHash) {
    const seq = await getNextSeq();
    const uuid = clientUuid || generateUUID();

    // حساب Hash على السيرفر
    let serverHash;
    try {
        serverHash = computeHash(data);
    } catch (e) {
        serverHash = 'error_computing_hash';
    }

    const hashToStore = serverHash;

    // تحقق من التطابق
    let hashValid = true;
    if (clientHash && clientHash !== serverHash) {
        console.warn(`⚠️ عدم تطابق Hash! client=${clientHash.substring(0, 12)}... server=${serverHash.substring(0, 12)}...`);
        hashValid = false;
    }

    const entry = {
        seq, uuid, type, data,
        hash: serverHash,
        hash_valid: hashValid,
        timestamp: new Date().toISOString(),
        sender: senderSocketId,
        acked_by: []
    };

    if (dbAvailable) {
        try {
            await pool.query(
                `INSERT INTO message_buffer (seq, uuid, type, data, hash, sender, timestamp, acked_by)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (seq) DO NOTHING`,
                [seq, uuid, type, JSON.stringify(data), hashToStore, senderSocketId, entry.timestamp, []]
            );
        } catch (err) {
            console.error('⚠️ فشل حفظ الرسالة:', err.message);
            // محاولة ثانية بدون uuid constraint
            try {
                await pool.query(
                    `INSERT INTO message_buffer (seq, type, data, hash, sender, timestamp, acked_by)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (seq) DO NOTHING`,
                    [seq, type, JSON.stringify(data), hashToStore, senderSocketId, entry.timestamp, []]
                );
                // ثم تحديث uuid
                await pool.query(
                    `UPDATE message_buffer SET uuid = $1 WHERE seq = $2 AND uuid IS NULL`,
                    [uuid, seq]
                );
            } catch (err2) {
                console.error('⚠️ فشل المحاولة الثانية:', err2.message);
            }
        }
    }

    return entry;
}

// ★ جلب رسائل بعد seq — مع حماية من أعمدة مفقودة
async function getBufferAfterSeq(afterSeq) {
    if (!dbAvailable) return [];
    try {
        // أولاً: فحص الأعمدة المتوفرة
        const columns = await getAvailableColumns('message_buffer');

        // بناء الاستعلام بناءً على الأعمدة الموجودة فعلاً
        const selectCols = ['seq', 'type', 'data'];
        if (columns.includes('uuid')) selectCols.push('uuid');
        if (columns.includes('hash')) selectCols.push('hash');
        if (columns.includes('sender')) selectCols.push('sender');
        if (columns.includes('timestamp')) selectCols.push('timestamp');
        if (columns.includes('acked_by')) selectCols.push('acked_by');

        const result = await pool.query(
            `SELECT ${selectCols.join(', ')} FROM message_buffer WHERE seq > $1 ORDER BY seq ASC`,
            [afterSeq]
        );

        return result.rows.map(row => ({
            seq: row.seq,
            uuid: row.uuid || generateUUID(),
            type: row.type,
            data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
            hash: row.hash || '',
            sender: row.sender || '',
            timestamp: row.timestamp || new Date().toISOString(),
            acked_by: row.acked_by || []
        }));
    } catch (err) {
        console.error('⚠️ فشل جلب الرسائل:', err.message);

        // ★★★ Fallback: جلب بدون الأعمدة المشكلة ★★★
        try {
            console.log('🔄 محاولة جلب بالأعمدة الأساسية فقط...');
            const result = await pool.query(
                `SELECT seq, type, data FROM message_buffer WHERE seq > $1 ORDER BY seq ASC`,
                [afterSeq]
            );
            return result.rows.map(row => ({
                seq: row.seq,
                uuid: generateUUID(),
                type: row.type,
                data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
                hash: '',
                sender: '',
                timestamp: new Date().toISOString(),
                acked_by: []
            }));
        } catch (err2) {
            console.error('❌ فشل الجلب نهائياً:', err2.message);
            return [];
        }
    }
}

// ★ دالة مساعدة: جلب أسماء الأعمدة المتوفرة
let _cachedColumns = {};
async function getAvailableColumns(tableName) {
    // cache لمدة 60 ثانية
    const now = Date.now();
    if (_cachedColumns[tableName] && (now - _cachedColumns[tableName].time < 60000)) {
        return _cachedColumns[tableName].columns;
    }
    try {
        const result = await pool.query(`
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = $1
        `, [tableName]);
        const columns = result.rows.map(r => r.column_name);
        _cachedColumns[tableName] = { columns, time: now };
        return columns;
    } catch (e) {
        return ['seq', 'type', 'data']; // الحد الأدنى
    }
}

// ★ جلب رسالة بالـ UUID
async function getMessageByUuid(uuid) {
    if (!dbAvailable || !uuid) return null;
    try {
        const columns = await getAvailableColumns('message_buffer');
        if (!columns.includes('uuid')) {
            console.warn('⚠️ عمود uuid غير موجود — لا يمكن البحث بالـ UUID');
            return null;
        }

        const result = await pool.query(
            'SELECT seq, uuid, type, data, hash FROM message_buffer WHERE uuid=$1',
            [uuid]
        );
        if (result.rows.length > 0) {
            const row = result.rows[0];
            return {
                seq: row.seq,
                uuid: row.uuid,
                type: row.type,
                data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
                hash: row.hash || ''
            };
        }
        return null;
    } catch (err) {
        console.error(`⚠️ خطأ البحث بـ UUID: ${err.message}`);
        return null;
    }
}

async function getBufferCount() {
    if (!dbAvailable) return 0;
    try {
        const result = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
        return parseInt(result.rows[0].count);
    } catch (err) { return 0; }
}

async function getBufferRange() {
    if (!dbAvailable) return { oldest: 0, newest: 0 };
    try {
        const result = await pool.query('SELECT MIN(seq) as oldest, MAX(seq) as newest FROM message_buffer');
        return { oldest: result.rows[0].oldest || 0, newest: result.rows[0].newest || 0 };
    } catch (err) { return { oldest: 0, newest: 0 }; }
}

async function markAcked(seq, socketId) {
    if (!dbAvailable) return;
    try {
        const columns = await getAvailableColumns('message_buffer');
        if (columns.includes('acked_by')) {
            await pool.query(
                `UPDATE message_buffer SET acked_by = array_append(acked_by, $1)
                 WHERE seq = $2 AND NOT ($1 = ANY(acked_by))`,
                [socketId, seq]
            );
        }
    } catch (err) { /* تجاهل */ }
}

async function updateMachineState(status, data) {
    machineStatus = status || machineStatus;
    if (data) {
        if (data.tailor) lastData.tailor = data.tailor;
        if (data.embroidery) lastData.color = data.embroidery;
        if (data.color) lastData.color = data.color;
        if (data.ficha_id) lastData.ficha_id = data.ficha_id;
    }
    if (dbAvailable) {
        try {
            await pool.query(
                'UPDATE machine_state SET status=$1, last_data=$2, updated_at=NOW() WHERE id=1',
                [machineStatus, JSON.stringify(lastData)]
            );
        } catch (err) { /* تجاهل */ }
    }
}

async function cleanOldMessages() {
    if (!dbAvailable) return;
    try {
        const result = await pool.query(
            `DELETE FROM message_buffer WHERE timestamp < NOW() - INTERVAL '15 days'`
        );
        if (result.rowCount > 0) {
            console.log(`🧹 تنظيف: حذف ${result.rowCount} رسالة قديمة`);
        }
    } catch (err) {
        // محاولة بدون عمود timestamp
        try {
            const countResult = await pool.query('SELECT COUNT(*) as c FROM message_buffer');
            const count = parseInt(countResult.rows[0].c);
            if (count > 10000) {
                await pool.query(
                    `DELETE FROM message_buffer WHERE seq < (SELECT MAX(seq) - 5000 FROM message_buffer)`
                );
                console.log(`🧹 تنظيف: حذف الرسائل القديمة (بالـ SEQ)`);
            }
        } catch (err2) { /* تجاهل */ }
    }
}

// ═══════════════════════════════════════════════════════
// ★ API endpoints
// ═══════════════════════════════════════════════════════
app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', async (req, res) => {
    const bufferCount = await getBufferCount();
    const range = await getBufferRange();
    res.json({
        status: 'ok',
        version: 'V5-UUID-Fix',
        clients: io.engine.clientsCount,
        machineStatus, lastData,
        bufferSize: bufferCount,
        lastSeq: sequenceCounter,
        oldestSeq: range.oldest,
        newestSeq: range.newest,
        dbAvailable,
        uptime: process.uptime()
    });
});

app.get('/api/sync', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    try {
        const messages = await getBufferAfterSeq(afterSeq);
        res.json({
            status: 'ok', last_seq: sequenceCounter,
            count: messages.length, messages,
            machine_status: machineStatus, last_data: lastData
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/api/buffer/status', async (req, res) => {
    const bufferCount = await getBufferCount();
    const range = await getBufferRange();

    // فحص الأعمدة
    let columns = [];
    try {
        columns = await getAvailableColumns('message_buffer');
    } catch (e) { }

    res.json({
        total_messages: bufferCount, last_seq: sequenceCounter,
        machine_status: machineStatus,
        oldest_seq: range.oldest, newest_seq: range.newest,
        db_available: dbAvailable,
        table_columns: columns
    });
});

app.post('/api/ack', async (req, res) => {
    const { seq, client_id } = req.body;
    if (seq && client_id) {
        await markAcked(seq, client_id);
        res.json({ status: 'ok', seq });
    } else {
        res.status(400).json({ status: 'error', message: 'seq and client_id required' });
    }
});

app.get('/api/resend/:uuid', async (req, res) => {
    const msg = await getMessageByUuid(req.params.uuid);
    if (msg) {
        res.json({ status: 'ok', message: msg });
    } else {
        res.status(404).json({ status: 'not_found' });
    }
});

app.get('/api/messages', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    const limit = Math.min(parseInt(req.query.limit) || 1000, 5000);
    try {
        const messages = await getBufferAfterSeq(afterSeq);
        // تطبيق الحد
        const limited = messages.slice(0, limit);
        res.json({ status: 'ok', count: limited.length, last_seq: sequenceCounter, messages: limited });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.get('/api/stats', async (req, res) => {
    try {
        let stats = {
            total_messages: 0, messages_today: 0,
            messages_by_type: {}, last_seq: sequenceCounter, db_available: dbAvailable
        };
        if (dbAvailable) {
            const totalResult = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
            stats.total_messages = parseInt(totalResult.rows[0].count);

            try {
                const todayResult = await pool.query(
                    "SELECT COUNT(*) as count FROM message_buffer WHERE timestamp >= CURRENT_DATE"
                );
                stats.messages_today = parseInt(todayResult.rows[0].count);
            } catch (e) {
                stats.messages_today = -1; // عمود timestamp قد لا يكون موجوداً
            }

            const typeResult = await pool.query(
                'SELECT type, COUNT(*) as count FROM message_buffer GROUP BY type ORDER BY count DESC'
            );
            typeResult.rows.forEach(row => { stats.messages_by_type[row.type] = parseInt(row.count); });
        }
        res.json(stats);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ API تشخيصي — لفحص بنية الجدول
app.get('/api/debug/schema', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'message_buffer' 
            ORDER BY ordinal_position
        `);
        res.json({
            status: 'ok',
            table: 'message_buffer',
            columns: result.rows
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ API لإعادة بناء الجدول (migration يدوي)
app.post('/api/admin/migrate', async (req, res) => {
    try {
        await initDatabase();
        // مسح cache الأعمدة
        _cachedColumns = {};
        res.json({ status: 'ok', message: 'Migration completed' });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════
// ★★★ Socket.IO V5 — UUID + Hash + NACK + حماية ★★★
// ═══════════════════════════════════════════════════════
io.on('connection', async (socket) => {
    console.log(`✅ متصل: ${socket.id} | الإجمالي: ${io.engine.clientsCount}`);

    const bufferCount = await getBufferCount();
    socket.emit('welcome', {
        status: machineStatus, details: lastData,
        last_seq: sequenceCounter, buffer_size: bufferCount,
        db_available: dbAvailable
    });
    socket.emit('machine_status', { status: machineStatus });

    // ═══ طلب مزامنة ═══
    socket.on('sync_request', async (payload) => {
        const afterSeq = (payload && payload.after_seq) || 0;
        console.log(`🔄 [${socket.id}] مزامنة بعد seq=${afterSeq}`);
        try {
            const missed = await getBufferAfterSeq(afterSeq);
            console.log(`   📦 إرسال ${missed.length} رسالة`);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: missed.length,
                messages: missed, machine_status: machineStatus, last_data: lastData
            });
        } catch (err) {
            console.error('❌ خطأ مزامنة:', err.message);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: 0, messages: [],
                machine_status: machineStatus, error: err.message
            });
        }
    });

    // ═══ تأكيد استلام ═══
    socket.on('ack', async (payload) => {
        if (!payload) return;
        const seq = payload.seq;
        if (seq) {
            await markAcked(seq, socket.id);
            // إشعار المرسل الأصلي
            if (dbAvailable) {
                try {
                    const columns = await getAvailableColumns('message_buffer');
                    if (columns.includes('sender')) {
                        const result = await pool.query(
                            'SELECT sender FROM message_buffer WHERE seq=$1', [seq]
                        );
                        if (result.rows.length > 0 && result.rows[0].sender) {
                            io.to(result.rows[0].sender).emit('delivery_confirmed', {
                                seq, acked_by: socket.id, timestamp: new Date().toISOString()
                            });
                        }
                    }
                } catch (err) { /* تجاهل */ }
            }
        }
    });

    socket.on('ack_batch', async (payload) => {
        if (!payload) return;
        const seqs = payload.seqs || [];
        for (const seq of seqs) { await markAcked(seq, socket.id); }
        console.log(`✅ [${socket.id}] ACK batch: ${seqs.length}`);
        if (dbAvailable) {
            try {
                const columns = await getAvailableColumns('message_buffer');
                if (columns.includes('sender')) {
                    const result = await pool.query(
                        'SELECT DISTINCT sender FROM message_buffer WHERE seq = ANY($1)', [seqs]
                    );
                    result.rows.forEach(row => {
                        if (row.sender) {
                            seqs.forEach(seq => {
                                io.to(row.sender).emit('delivery_confirmed', { seq, acked_by: socket.id });
                            });
                        }
                    });
                }
            } catch (err) { /* تجاهل */ }
        }
    });

    // ═══ طلب إعادة إرسال (NACK) ═══
    socket.on('nack_resend', async (payload) => {
        if (!payload) return;
        const uuid = payload.uuid;
        const reason = payload.reason || 'unknown';
        console.log(`🔁 [${socket.id}] NACK uuid=${uuid} reason=${reason}`);

        if (uuid) {
            const msg = await getMessageByUuid(uuid);
            if (msg) {
                socket.emit('resend_message', {
                    seq: msg.seq, uuid: msg.uuid, type: msg.type,
                    data: msg.data, hash: msg.hash,
                    is_resend: true
                });
                console.log(`   ✅ أُعيد إرسال uuid=${uuid}`);
            } else {
                socket.emit('resend_failed', { uuid, reason: 'not_found' });
                console.log(`   ❌ uuid=${uuid} غير موجود`);
            }
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ أوامر الآلة ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('command', async (payload) => {
        if (!payload) return;
        console.log(`📨 [${socket.id}] command:`, JSON.stringify(payload).substring(0, 200));

        const action = payload.action;
        const data = payload.data || payload.details || {};
        const clientUuid = payload.uuid;
        const clientHash = payload.hash;

        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            await updateMachineState(action, data);

            const entry = await addToBuffer('machine_command', {
                action, speed: data.speed || null, timestamp: payload.timestamp
            }, socket.id, clientUuid, clientHash);

            socket.broadcast.emit('machine_status', {
                status: action, speed: data.speed || null,
                timestamp: payload.timestamp,
                seq: entry.seq, uuid: entry.uuid, hash: entry.hash
            });

            io.emit('update_ui', {
                status: machineStatus, details: lastData,
                seq: entry.seq, uuid: entry.uuid
            });

            socket.emit('server_ack', {
                seq: entry.seq, uuid: entry.uuid,
                original_action: action, status: 'stored_in_db',
                hash_valid: entry.hash_valid
            });
        }

        if (action === 'data_change') {
            const changeType = payload.change_type;
            const details = payload.details || {};
            await updateMachineState(null, details);

            const entry = await addToBuffer(changeType, details, socket.id, clientUuid, clientHash);

            socket.broadcast.emit('data_changed', {
                type: changeType, change_type: changeType,
                details, timestamp: payload.timestamp,
                seq: entry.seq, uuid: entry.uuid, hash: entry.hash
            });

            io.emit('update_ui', {
                status: machineStatus, details: lastData,
                seq: entry.seq, uuid: entry.uuid
            });

            socket.emit('server_ack', {
                seq: entry.seq, uuid: entry.uuid,
                original_action: changeType, status: 'stored_in_db',
                hash_valid: entry.hash_valid
            });
        }

        if (action === 'SYNC_REQUEST') {
            const afterSeq = (data && data.after_seq) || 0;
            const missed = await getBufferAfterSeq(afterSeq);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: missed.length,
                messages: missed, machine_status: machineStatus, last_data: lastData
            });
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ أحداث مباشرة — V5 مع UUID + Hash ★★★
    // ═══════════════════════════════════════════════════════
    async function handleDirectEvent(eventName, data, socket) {
        if (!data || typeof data !== 'object') {
            console.warn(`⚠️ [${eventName}] بيانات غير صالحة`);
            return;
        }

        const clientUuid = data.uuid || data._uuid;
        const clientHash = data.hash || data._hash;

        // تحقق من عدم التكرار بالـ UUID
        if (clientUuid && dbAvailable) {
            try {
                const columns = await getAvailableColumns('message_buffer');
                if (columns.includes('uuid')) {
                    const existing = await pool.query(
                        'SELECT seq FROM message_buffer WHERE uuid=$1', [clientUuid]
                    );
                    if (existing.rows.length > 0) {
                        console.log(`   🔄 UUID مكرر: ${clientUuid.substring(0, 8)}... — تجاهل`);
                        socket.emit('server_ack', {
                            seq: existing.rows[0].seq, uuid: clientUuid,
                            original_action: eventName, status: 'already_exists'
                        });
                        return;
                    }
                }
            } catch (err) { /* واصل */ }
        }

        // احذف الحقول الخاصة
        const cleanData = { ...data };
        delete cleanData._uuid;
        delete cleanData._hash;
        delete cleanData.uuid;
        delete cleanData.hash;

        const entry = await addToBuffer(eventName, cleanData, socket.id, clientUuid, clientHash);
        await updateMachineState(null, cleanData);

        // بث
        const broadcastData = {
            ...cleanData,
            seq: entry.seq, uuid: entry.uuid, hash: entry.hash
        };

        socket.broadcast.emit(eventName, broadcastData);
        io.emit('update_ui', {
            status: machineStatus, details: lastData,
            seq: entry.seq, uuid: entry.uuid
        });
        socket.emit('server_ack', {
            seq: entry.seq, uuid: entry.uuid,
            original_action: eventName, status: 'stored_in_db',
            hash_valid: entry.hash_valid
        });

        console.log(`   📡 ${eventName} [seq=${entry.seq}, uuid=${(entry.uuid || '').substring(0, 8)}...] ✅`);
    }

    socket.on('ficha_saved', async (data) => {
        console.log(`📋 [${socket.id}] ficha_saved`);
        await handleDirectEvent('ficha_saved', data, socket);
    });

    socket.on('ficha_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted`);
        await handleDirectEvent('ficha_deleted', data, socket);
    });

    socket.on('production_saved', async (data) => {
        console.log(`🧺 [${socket.id}] production_saved`);
        await handleDirectEvent('production_saved', data, socket);
    });

    socket.on('production_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted`);
        await handleDirectEvent('production_deleted', data, socket);
    });

    socket.on('machine_status', async (data) => {
        console.log(`⚙️ [${socket.id}] machine_status`);
        if (data && data.status) await updateMachineState(data.status, null);
        await handleDirectEvent('machine_status', data, socket);
    });

    socket.on('speed_update', async (data) => {
        if (!data) return;
        const entry = await addToBuffer('speed_update', data, socket.id, data.uuid, data.hash);
        data.seq = entry.seq;
        data.uuid = entry.uuid;
        data.hash = entry.hash;
        socket.broadcast.emit('speed_update', data);
    });

    socket.on('technical_data', async (data) => {
        console.log(`🔧 [${socket.id}] technical_data`);
        await handleDirectEvent('technical_data', data, socket);
    });

    socket.on('coordinates_batch', async (data) => {
        console.log(`📍 [${socket.id}] coordinates_batch`);
        await handleDirectEvent('coordinates_batch', data, socket);
    });

    socket.on('head_command', async (data) => {
        console.log(`🎯 [${socket.id}] head_command`);
        await handleDirectEvent('head_command', data, socket);
    });

    socket.on('disconnect', () => {
        console.log(`❌ انقطع: ${socket.id} | المتبقي: ${io.engine.clientsCount}`);
    });
});

// تنظيف دوري كل ساعة
setInterval(cleanOldMessages, 60 * 60 * 1000);

// ═══════════════════════════════════════════════════════
// ★★★ بدء التشغيل ★★★
// ═══════════════════════════════════════════════════════
const PORT = process.env.PORT || 10000;

async function startServer() {
    await initDatabase();
    await loadStateFromDB();
    await cleanOldMessages();

    // مسح cache بعد الـ migration
    _cachedColumns = {};

    server.listen(PORT, () => {
        console.log('═'.repeat(65));
        console.log(`🚀 السيرفر V5-UUID-Fix جاهز على المنفذ ${PORT}`);
        console.log(`💾 PostgreSQL: ${dbAvailable ? '✅' : '❌'}`);
        console.log(`🔐 UUID + SHA-256 Hash لكل رسالة`);
        console.log(`🔁 دعم NACK/Resend`);
        console.log(`🔧 Auto-Migration: أعمدة مفقودة تُضاف تلقائياً`);
        console.log(`📊 API تشخيصي: /api/debug/schema`);
        console.log('═'.repeat(65));
    });
}

startServer().catch(err => {
    console.error('❌ فشل البدء:', err);
    process.exit(1);
});
