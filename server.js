const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['polling', 'websocket'],
    pingTimeout: 60000,
    pingInterval: 25000
});

// ═══════════════════════════════════════════════════════
// ★★★ إعداد PostgreSQL ★★★
// ═══════════════════════════════════════════════════════
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false,
    },
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

// ★ معالجة أخطاء الاتصال بشكل عام (يمنع توقف السيرفر)
pool.on('error', (err) => {
    console.error('⚠️ خطأ غير متوقع في اتصال PostgreSQL:', err.message);
});

// ═══════════════════════════════════════════════════════
// ★★★ إنشاء الجداول تلقائياً عند التشغيل ★★★
// ═══════════════════════════════════════════════════════
async function initDatabase() {
    const client = await pool.connect();
    try {
        // جدول الرسائل المخزنة (Buffer دائم)
        await client.query(`
            CREATE TABLE IF NOT EXISTS message_buffer (
                id SERIAL PRIMARY KEY,
                seq INTEGER UNIQUE NOT NULL,
                type VARCHAR(100) NOT NULL,
                data JSONB NOT NULL,
                sender VARCHAR(100),
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                acked_by TEXT[] DEFAULT '{}'
            )
        `);

        // جدول حالة الآلة
        await client.query(`
            CREATE TABLE IF NOT EXISTS machine_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                status VARCHAR(50) DEFAULT 'UNKNOWN',
                speed REAL DEFAULT 0,
                last_data JSONB DEFAULT '{"tailor":"---","color":"---","ficha_id":"---"}',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        `);

        // جدول العداد التسلسلي
        await client.query(`
            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                current_seq INTEGER DEFAULT 0
            )
        `);

        // فهرس للبحث السريع
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_buffer_seq ON message_buffer(seq)
        `);
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_buffer_timestamp ON message_buffer(timestamp)
        `);
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_buffer_type ON message_buffer(type)
        `);

        // إدراج السجلات الأولية إذا لم تكن موجودة
        await client.query(`
            INSERT INTO machine_state (id, status, last_data)
            VALUES (1, 'UNKNOWN', '{"tailor":"---","color":"---","ficha_id":"---"}')
            ON CONFLICT (id) DO NOTHING
        `);
        await client.query(`
            INSERT INTO sequence_counter (id, current_seq)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        `);

        console.log('✅ قاعدة البيانات PostgreSQL جاهزة');

        // عرض إحصائيات
        const countResult = await client.query('SELECT COUNT(*) as count FROM message_buffer');
        const seqResult = await client.query('SELECT current_seq FROM sequence_counter WHERE id=1');
        console.log(`   📦 الرسائل المخزنة: ${countResult.rows[0].count}`);
        console.log(`   🔢 آخر SEQ: ${seqResult.rows[0]?.current_seq || 0}`);

    } catch (err) {
        console.error('❌ خطأ في إنشاء قاعدة البيانات:', err.message);
        console.log('⚠️ سيعمل السيرفر بدون قاعدة بيانات (Buffer في الذاكرة فقط)');
    } finally {
        client.release();
    }
}

// ═══════════════════════════════════════════════════════
// ★★★ دوال قاعدة البيانات ★★★
// ═══════════════════════════════════════════════════════

// حالة مشتركة (cache في الذاكرة)
let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', color: '---', ficha_id: '---' };
let sequenceCounter = 0;
let dbAvailable = false;

// ★ تحميل الحالة من قاعدة البيانات
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
        console.log(`✅ تم تحميل الحالة: status=${machineStatus}, seq=${sequenceCounter}`);
    } catch (err) {
        console.error('⚠️ فشل تحميل الحالة:', err.message);
        dbAvailable = false;
    }
}

// ★ الحصول على رقم تسلسلي جديد
async function getNextSeq() {
    sequenceCounter++;
    if (dbAvailable) {
        try {
            await pool.query(
                'UPDATE sequence_counter SET current_seq = $1 WHERE id = 1',
                [sequenceCounter]
            );
        } catch (err) {
            console.error('⚠️ فشل تحديث SEQ:', err.message);
        }
    }
    return sequenceCounter;
}

// ★ إضافة رسالة إلى Buffer الدائم
async function addToBuffer(type, data, senderSocketId) {
    const seq = await getNextSeq();
    const entry = {
        seq: seq,
        type: type,
        data: data,
        timestamp: new Date().toISOString(),
        sender: senderSocketId,
        acked_by: []
    };

    if (dbAvailable) {
        try {
            await pool.query(
                `INSERT INTO message_buffer (seq, type, data, sender, timestamp, acked_by)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (seq) DO NOTHING`,
                [seq, type, JSON.stringify(data), senderSocketId, entry.timestamp, []]
            );
        } catch (err) {
            console.error('⚠️ فشل حفظ الرسالة في DB:', err.message);
        }
    }

    return entry;
}

// ★ جلب الرسائل بعد رقم تسلسل معين
async function getBufferAfterSeq(afterSeq) {
    if (!dbAvailable) return [];
    try {
        const result = await pool.query(
            'SELECT seq, type, data, sender, timestamp, acked_by FROM message_buffer WHERE seq > $1 ORDER BY seq ASC',
            [afterSeq]
        );
        return result.rows.map(row => ({
            seq: row.seq,
            type: row.type,
            data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
            sender: row.sender,
            timestamp: row.timestamp,
            acked_by: row.acked_by || []
        }));
    } catch (err) {
        console.error('⚠️ فشل جلب الرسائل:', err.message);
        return [];
    }
}

// ★ جلب عدد الرسائل
async function getBufferCount() {
    if (!dbAvailable) return 0;
    try {
        const result = await pool.query('SELECT COUNT(*) as count FROM message_buffer');
        return parseInt(result.rows[0].count);
    } catch (err) {
        return 0;
    }
}

// ★ جلب نطاق الـ SEQ
async function getBufferRange() {
    if (!dbAvailable) return { oldest: 0, newest: 0 };
    try {
        const result = await pool.query(
            'SELECT MIN(seq) as oldest, MAX(seq) as newest FROM message_buffer'
        );
        return {
            oldest: result.rows[0].oldest || 0,
            newest: result.rows[0].newest || 0
        };
    } catch (err) {
        return { oldest: 0, newest: 0 };
    }
}

// ★ تسجيل تأكيد الاستلام
async function markAcked(seq, socketId) {
    if (!dbAvailable) return;
    try {
        await pool.query(
            `UPDATE message_buffer
             SET acked_by = array_append(acked_by, $1)
             WHERE seq = $2 AND NOT ($1 = ANY(acked_by))`,
            [socketId, seq]
        );
    } catch (err) {
        console.error('⚠️ فشل تسجيل ACK:', err.message);
    }
}

// ★ تحديث حالة الآلة في DB
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
                `UPDATE machine_state
                 SET status = $1, last_data = $2, updated_at = NOW()
                 WHERE id = 1`,
                [machineStatus, JSON.stringify(lastData)]
            );
        } catch (err) {
            console.error('⚠️ فشل تحديث حالة الآلة:', err.message);
        }
    }
}

// ★ تنظيف الرسائل القديمة (أقدم من 7 أيام)
async function cleanOldMessages() {
    if (!dbAvailable) return;
    try {
        const result = await pool.query(
            `DELETE FROM message_buffer
             WHERE timestamp < NOW() - INTERVAL '7 days'`
        );
        if (result.rowCount > 0) {
            console.log(`🧹 تنظيف: حذف ${result.rowCount} رسالة أقدم من 7 أيام`);
        }
    } catch (err) {
        console.error('⚠️ فشل التنظيف:', err.message);
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
        clients: io.engine.clientsCount,
        machineStatus: machineStatus,
        lastData: lastData,
        bufferSize: bufferCount,
        lastSeq: sequenceCounter,
        oldestSeq: range.oldest,
        newestSeq: range.newest,
        dbAvailable: dbAvailable,
        uptime: process.uptime()
    });
});

// ★ API للمزامنة عبر HTTP
app.get('/api/sync', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    try {
        const messages = await getBufferAfterSeq(afterSeq);
        res.json({
            status: 'ok',
            last_seq: sequenceCounter,
            count: messages.length,
            messages: messages,
            machine_status: machineStatus,
            last_data: lastData
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ API لجلب حالة الـ Buffer
app.get('/api/buffer/status', async (req, res) => {
    const bufferCount = await getBufferCount();
    const range = await getBufferRange();
    res.json({
        total_messages: bufferCount,
        last_seq: sequenceCounter,
        machine_status: machineStatus,
        oldest_seq: range.oldest,
        newest_seq: range.newest,
        db_available: dbAvailable
    });
});

// ★ API لتأكيد الاستلام عبر HTTP
app.post('/api/ack', async (req, res) => {
    const { seq, client_id } = req.body;
    if (seq && client_id) {
        await markAcked(seq, client_id);
        res.json({ status: 'ok', seq: seq });
    } else {
        res.status(400).json({ status: 'error', message: 'seq and client_id required' });
    }
});

// ★ API لجلب رسائل بنطاق معين
app.get('/api/messages', async (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    const limit = Math.min(parseInt(req.query.limit) || 1000, 5000);
    try {
        let messages;
        if (dbAvailable) {
            const result = await pool.query(
                'SELECT seq, type, data, sender, timestamp FROM message_buffer WHERE seq > $1 ORDER BY seq ASC LIMIT $2',
                [afterSeq, limit]
            );
            messages = result.rows.map(row => ({
                seq: row.seq,
                type: row.type,
                data: typeof row.data === 'string' ? JSON.parse(row.data) : row.data,
                timestamp: row.timestamp
            }));
        } else {
            messages = [];
        }
        res.json({
            status: 'ok',
            count: messages.length,
            last_seq: sequenceCounter,
            messages: messages
        });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ★ API للإحصائيات
app.get('/api/stats', async (req, res) => {
    try {
        let stats = {
            total_messages: 0,
            messages_today: 0,
            messages_by_type: {},
            last_seq: sequenceCounter,
            db_available: dbAvailable
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
            typeResult.rows.forEach(row => {
                stats.messages_by_type[row.type] = parseInt(row.count);
            });
        }

        res.json(stats);
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ═══════════════════════════════════════════════════════
// ★★★ Socket.IO — مع PostgreSQL Buffer ★★★
// ═══════════════════════════════════════════════════════
io.on('connection', async (socket) => {
    console.log(`✅ متصل جديد: ${socket.id} | الإجمالي: ${io.engine.clientsCount}`);

    // ★ إرسال آخر حالة + رقم التسلسل الحالي
    const bufferCount = await getBufferCount();
    socket.emit('welcome', {
        status: machineStatus,
        details: lastData,
        last_seq: sequenceCounter,
        buffer_size: bufferCount,
        db_available: dbAvailable
    });
    socket.emit('machine_status', { status: machineStatus });

    // ═══════════════════════════════════════════════════════
    // ★★★ طلب مزامنة ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('sync_request', async (payload) => {
        const afterSeq = payload.after_seq || 0;
        console.log(`🔄 [${socket.id}] طلب مزامنة بعد seq=${afterSeq}`);

        try {
            const missed = await getBufferAfterSeq(afterSeq);
            console.log(`   📦 إرسال ${missed.length} رسالة فائتة`);

            socket.emit('sync_response', {
                last_seq: sequenceCounter,
                count: missed.length,
                messages: missed,
                machine_status: machineStatus,
                last_data: lastData
            });
        } catch (err) {
            console.error('❌ خطأ مزامنة:', err.message);
            socket.emit('sync_response', {
                last_seq: sequenceCounter,
                count: 0,
                messages: [],
                machine_status: machineStatus,
                error: err.message
            });
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ تأكيد الاستلام ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('ack', async (payload) => {
        const seq = payload.seq;
        if (seq) {
            await markAcked(seq, socket.id);
            // إبلاغ المرسل
            if (dbAvailable) {
                try {
                    const result = await pool.query(
                        'SELECT sender FROM message_buffer WHERE seq = $1', [seq]
                    );
                    if (result.rows.length > 0 && result.rows[0].sender) {
                        io.to(result.rows[0].sender).emit('delivery_confirmed', {
                            seq: seq,
                            acked_by: socket.id,
                            timestamp: new Date().toISOString()
                        });
                    }
                } catch (err) { /* تجاهل */ }
            }
        }
    });

    socket.on('ack_batch', async (payload) => {
        const seqs = payload.seqs || [];
        for (const seq of seqs) {
            await markAcked(seq, socket.id);
        }
        console.log(`✅ [${socket.id}] تأكيد استلام ${seqs.length} رسالة`);

        // إبلاغ المرسلين
        if (dbAvailable) {
            try {
                const result = await pool.query(
                    'SELECT DISTINCT sender FROM message_buffer WHERE seq = ANY($1)',
                    [seqs]
                );
                result.rows.forEach(row => {
                    if (row.sender) {
                        seqs.forEach(seq => {
                            io.to(row.sender).emit('delivery_confirmed', {
                                seq: seq,
                                acked_by: socket.id
                            });
                        });
                    }
                });
            } catch (err) { /* تجاهل */ }
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ استقبال الأوامر ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('command', async (payload) => {
        console.log(`📨 [${socket.id}] command:`, JSON.stringify(payload).substring(0, 200));

        const action = payload.action;
        const data = payload.data || payload.details || {};

        // ─── أوامر الآلة ───
        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            await updateMachineState(action, data);

            const entry = await addToBuffer('machine_command', {
                action: action,
                speed: data.speed || null,
                timestamp: payload.timestamp
            }, socket.id);

            socket.broadcast.emit('machine_status', {
                status: action,
                speed: data.speed || null,
                timestamp: payload.timestamp,
                seq: entry.seq
            });

            io.emit('update_ui', {
                status: machineStatus,
                details: lastData,
                seq: entry.seq
            });

            socket.emit('server_ack', {
                seq: entry.seq,
                original_action: action,
                status: 'stored_in_db'
            });

            console.log(`   ⚙️ حالة الآلة: ${action} [seq=${entry.seq}] ✅ DB`);
        }

        // ─── تغيير البيانات ───
        if (action === 'data_change') {
            const changeType = payload.change_type;
            const details = payload.details || {};

            await updateMachineState(null, details);

            const entry = await addToBuffer(changeType, details, socket.id);

            socket.broadcast.emit('data_changed', {
                type: changeType,
                change_type: changeType,
                details: details,
                timestamp: payload.timestamp,
                seq: entry.seq
            });

            io.emit('update_ui', {
                status: machineStatus,
                details: lastData,
                seq: entry.seq
            });

            socket.emit('server_ack', {
                seq: entry.seq,
                original_action: changeType,
                status: 'stored_in_db'
            });

            console.log(`   📡 ${changeType} [seq=${entry.seq}] ✅ DB`);
        }

        // ─── طلب مزامنة ───
        if (action === 'SYNC_REQUEST') {
            const afterSeq = data.after_seq || 0;
            const missed = await getBufferAfterSeq(afterSeq);

            socket.emit('sync_response', {
                last_seq: sequenceCounter,
                count: missed.length,
                messages: missed,
                machine_status: machineStatus,
                last_data: lastData
            });

            socket.emit('update_ui', {
                status: machineStatus,
                details: lastData,
                last_seq: sequenceCounter
            });

            console.log(`   🔄 مزامنة: ${missed.length} رسالة بعد seq=${afterSeq}`);
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ أحداث مباشرة — مع حفظ في PostgreSQL ★★★
    // ═══════════════════════════════════════════════════════

    socket.on('ficha_saved', async (data) => {
        console.log(`📋 [${socket.id}] ficha_saved`);
        const entry = await addToBuffer('ficha_saved', data, socket.id);
        data.seq = entry.seq;
        await updateMachineState(null, data);
        socket.broadcast.emit('ficha_saved', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });
        socket.emit('server_ack', { seq: entry.seq, original_action: 'ficha_saved', status: 'stored_in_db' });
    });

    socket.on('ficha_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted`);
        const entry = await addToBuffer('ficha_deleted', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('ficha_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });
        socket.emit('server_ack', { seq: entry.seq, original_action: 'ficha_deleted', status: 'stored_in_db' });
    });

    socket.on('production_saved', async (data) => {
        console.log(`🧺 [${socket.id}] production_saved`);
        const entry = await addToBuffer('production_saved', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('production_saved', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });
        socket.emit('server_ack', { seq: entry.seq, original_action: 'production_saved', status: 'stored_in_db' });
    });

    socket.on('production_deleted', async (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted`);
        const entry = await addToBuffer('production_deleted', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('production_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });
        socket.emit('server_ack', { seq: entry.seq, original_action: 'production_deleted', status: 'stored_in_db' });
    });

    socket.on('machine_status', async (data) => {
        console.log(`⚙️ [${socket.id}] machine_status`);
        if (data.status) await updateMachineState(data.status, null);
        const entry = await addToBuffer('machine_status', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('machine_status', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });
        socket.emit('server_ack', { seq: entry.seq, original_action: 'machine_status', status: 'stored_in_db' });
    });

    socket.on('speed_update', async (data) => {
        const entry = await addToBuffer('speed_update', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('speed_update', data);
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ بيانات تقنية ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('technical_data', async (data) => {
        console.log(`🔧 [${socket.id}] technical_data: ${data.data_type || 'unknown'}`);
        const entry = await addToBuffer('technical_data', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('technical_data', data);
        socket.emit('server_ack', { seq: entry.seq, original_action: 'technical_data', status: 'stored_in_db' });
    });

    socket.on('coordinates_batch', async (data) => {
        console.log(`📍 [${socket.id}] coordinates_batch: ${(data.coordinates || []).length} نقطة`);
        const entry = await addToBuffer('coordinates_batch', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('coordinates_batch', data);
        socket.emit('server_ack', { seq: entry.seq, original_action: 'coordinates_batch', status: 'stored_in_db' });
    });

    socket.on('head_command', async (data) => {
        console.log(`🎯 [${socket.id}] head_command: ${data.command || 'unknown'}`);
        const entry = await addToBuffer('head_command', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('head_command', data);
        socket.emit('server_ack', { seq: entry.seq, original_action: 'head_command', status: 'stored_in_db' });
    });

    socket.on('disconnect', () => {
        console.log(`❌ انقطع: ${socket.id} | المتبقي: ${io.engine.clientsCount}`);
    });
});

// ═══════════════════════════════════════════════════════
// ★ تنظيف دوري (كل ساعة) — حذف أقدم من 7 أيام
// ═══════════════════════════════════════════════════════
setInterval(cleanOldMessages, 60 * 60 * 1000);

// ═══════════════════════════════════════════════════════
// ★★★ بدء التشغيل ★★★
// ═══════════════════════════════════════════════════════
const PORT = process.env.PORT || 10000;

async function startServer() {
    // تهيئة قاعدة البيانات
    await initDatabase();
    await loadStateFromDB();

    // تنظيف أولي
    await cleanOldMessages();

    server.listen(PORT, () => {
        console.log('═'.repeat(65));
        console.log(`🚀 السيرفر جاهز على المنفذ ${PORT}`);
        console.log(`💾 PostgreSQL: ${dbAvailable ? '✅ متصل' : '❌ غير متصل'}`);
        console.log(`📡 الأحداث المدعومة:`);
        console.log(`   command, ficha_saved, ficha_deleted`);
        console.log(`   production_saved, production_deleted`);
        console.log(`   machine_status, speed_update`);
        console.log(`   technical_data, coordinates_batch, head_command`);
        console.log(`   sync_request, ack, ack_batch`);
        console.log(`📦 حفظ دائم في PostgreSQL — لا تضيع البيانات!`);
        console.log(`🧹 تنظيف تلقائي: كل ساعة (حذف أقدم من 7 أيام)`);
        console.log('═'.repeat(65));
    });
}

startServer().catch(err => {
    console.error('❌ فشل بدء التشغيل:', err);
    process.exit(1);
});