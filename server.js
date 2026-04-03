const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');
const crypto = require('crypto');

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
// ★★★ إنشاء الجداول ★★★
// ═══════════════════════════════════════════════════════
async function initDatabase() {
    const client = await pool.connect();
    try {
        // جدول الرسائل — مع UUID و Hash
        await client.query(`
            CREATE TABLE IF NOT EXISTS message_buffer (
                id SERIAL PRIMARY KEY,
                seq INTEGER UNIQUE NOT NULL,
                uuid VARCHAR(36) UNIQUE NOT NULL,
                type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                hash VARCHAR(64) NOT NULL,
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

        // فهارس
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_seq ON message_buffer(seq)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_uuid ON message_buffer(uuid)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_timestamp ON message_buffer(timestamp)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_buffer_type ON message_buffer(type)`);

        await client.query(`
            INSERT INTO machine_state (id, status, last_data)
            VALUES (1, 'UNKNOWN', '{"tailor":"---","color":"---","ficha_id":"---"}')
            ON CONFLICT (id) DO NOTHING
        `);
        await client.query(`
            INSERT INTO sequence_counter (id, current_seq) VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        `);

        console.log('✅ قاعدة البيانات PostgreSQL جاهزة');
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        console.log(`   📦 الرسائل المخزنة: ${countResult.rows[0].count}`);
        console.log(`   🔢 آخر SEQ: ${seqResult.rows[0]?.current_seq || 0}`);
    } catch (err) {
        console.error('❌ خطأ إنشاء DB:', err.message);
    } finally {
        client.release();
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

// ★ حساب Hash على السيرفر
function computeHash(data) {
    const str = JSON.stringify(data, Object.keys(data).sort());
    return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
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

// ★ إضافة رسالة — مع UUID + Hash
async function addToBuffer(type, data, senderSocketId, clientUuid, clientHash) {
    const seq = await getNextSeq();

    // إذا لم يُرسل UUID من العميل، أنشئ واحداً
    const uuid = clientUuid || crypto.randomUUID();

    // تحقق من Hash العميل
    const serverHash = computeHash(data);
    const hashToStore = clientHash || serverHash;

    // تحقق من التطابق إذا أرسل العميل hash
    let hashValid = true;
    if (clientHash && clientHash !== serverHash) {
        console.warn(`⚠️ عدم تطابق Hash! client=${clientHash.substring(0,12)}... server=${serverHash.substring(0,12)}...`);
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
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT (uuid) DO NOTHING`,
                [seq, uuid, type, JSON.stringify(data), serverHash, senderSocketId, entry.timestamp, []]
            );
        } catch (err) {
            console.error('⚠️ فشل حفظ الرسالة:', err.message);
        }
    }

    return entry;
}

// ★ جلب رسائل بعد seq
async function getBufferAfterSeq(afterSeq) {
    if (!dbAvailable) return [];
    try {
        const result = await pool.query(
            `SELECT seq, uuid, type, data, hash, sender, timestamp, acked_by
             FROM message_buffer WHERE seq > $1 ORDER BY seq ASC`,
            [afterSeq]
        );
        return result.rows.map(row => ({
            seq: row.seq,
            uuid: row.uuid,
            type: row.type,
            data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
            hash: row.hash,
            sender: row.sender,
            timestamp: row.timestamp,
            acked_by: row.acked_by || []
        }));
    } catch (err) {
        console.error('⚠️ فشل جلب الرسائل:', err.message);
        return [];
    }
}

// ★ جلب رسالة بالـ UUID
async function getMessageByUuid(uuid) {
    if (!dbAvailable) return null;
    try {
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
                hash: row.hash
            };
        }
        return null;
    } catch (err) {
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
        await pool.query(
            `UPDATE message_buffer SET acked_by = array_append(acked_by, $1)
             WHERE seq = $2 AND NOT ($1 = ANY(acked_by))`,
            [socketId, seq]
        );
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
    } catch (err) { /* تجاهل */ }
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
    res.json({
        total_messages: bufferCount, last_seq: sequenceCounter,
        machine_status: machineStatus,
        oldest_seq: range.oldest, newest_seq: range.newest,
        db_available: dbAvailable
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

// ★ API لإعادة إرسال رسالة بالـ UUID
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
        let messages;
        if (dbAvailable) {
            const result = await pool.query(
                `SELECT seq, uuid, type, data, hash, sender, timestamp
                 FROM message_buffer WHERE seq > $1 ORDER BY seq ASC LIMIT $2`,
                [afterSeq, limit]
            );
            messages = result.rows.map(row => ({
                seq: row.seq, uuid: row.uuid, type: row.type,
                data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
                hash: row.hash, timestamp: row.timestamp
            }));
        } else {
            messages = [];
        }
        res.json({ status: 'ok', count: messages.length, last_seq: sequenceCounter, messages });
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
            const todayResult = await pool.query(
                "SELECT COUNT(*) as count FROM message_buffer WHERE timestamp >= CURRENT_DATE"
            );
            stats.messages_today = parseInt(todayResult.rows[0].count);
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

// ═══════════════════════════════════════════════════════
// ★★★ Socket.IO V9 — UUID + Hash + NACK ★★★
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
        const afterSeq = payload.after_seq || 0;
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
        const seq = payload.seq;
        if (seq) {
            await markAcked(seq, socket.id);
            if (dbAvailable) {
                try {
                    const result = await pool.query(
                        'SELECT sender FROM message_buffer WHERE seq=$1', [seq]
                    );
                    if (result.rows.length > 0 && result.rows[0].sender) {
                        io.to(result.rows[0].sender).emit('delivery_confirmed', {
                            seq, acked_by: socket.id, timestamp: new Date().toISOString()
                        });
                    }
                } catch (err) { /* تجاهل */ }
            }
        }
    });

    socket.on('ack_batch', async (payload) => {
        const seqs = payload.seqs || [];
        for (const seq of seqs) { await markAcked(seq, socket.id); }
        console.log(`✅ [${socket.id}] ACK batch: ${seqs.length}`);
        if (dbAvailable) {
            try {
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
            } catch (err) { /* تجاهل */ }
        }
    });

    // ═══ طلب إعادة إرسال (NACK) ═══
    socket.on('nack_resend', async (payload) => {
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
            const afterSeq = data.after_seq || 0;
            const missed = await getBufferAfterSeq(afterSeq);
            socket.emit('sync_response', {
                last_seq: sequenceCounter, count: missed.length,
                messages: missed, machine_status: machineStatus, last_data: lastData
            });
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ أحداث مباشرة — V9 مع UUID + Hash ★★★
    // ═══════════════════════════════════════════════════════

    // دالة مساعدة لمعالجة الأحداث المباشرة
    async function handleDirectEvent(eventName, data, socket) {
        const clientUuid = data.uuid || data._uuid;
        const clientHash = data.hash || data._hash;

        // تحقق من عدم التكرار بالـ UUID
        if (clientUuid && dbAvailable) {
            try {
                const existing = await pool.query(
                    'SELECT seq FROM message_buffer WHERE uuid=$1', [clientUuid]
                );
                if (existing.rows.length > 0) {
                    console.log(`   🔄 UUID مكرر: ${clientUuid} — تجاهل`);
                    socket.emit('server_ack', {
                        seq: existing.rows[0].seq, uuid: clientUuid,
                        original_action: eventName, status: 'already_exists'
                    });
                    return;
                }
            } catch (err) { /* واصل */ }
        }

        // احذف الحقول الخاصة من البيانات قبل التخزين
        const cleanData = { ...data };
        delete cleanData._uuid;
        delete cleanData._hash;
        delete cleanData.uuid;
        delete cleanData.hash;

        const entry = await addToBuffer(eventName, cleanData, socket.id, clientUuid, clientHash);
        await updateMachineState(null, cleanData);

        // أضف UUID و Hash للبث
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

        console.log(`   📡 ${eventName} [seq=${entry.seq}, uuid=${entry.uuid.substring(0,8)}...] ✅`);
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
        if (data.status) await updateMachineState(data.status, null);
        await handleDirectEvent('machine_status', data, socket);
    });

    socket.on('speed_update', async (data) => {
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

    server.listen(PORT, () => {
        console.log('═'.repeat(65));
        console.log(`🚀 السيرفر V9 جاهز على المنفذ ${PORT}`);
        console.log(`💾 PostgreSQL: ${dbAvailable ? '✅' : '❌'}`);
        console.log(`🔐 UUID + SHA-256 Hash لكل رسالة`);
        console.log(`🔁 دعم NACK/Resend`);
        console.log('═'.repeat(65));
    });
}

startServer().catch(err => {
    console.error('❌ فشل البدء:', err);
    process.exit(1);
});