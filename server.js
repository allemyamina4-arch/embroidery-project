const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['polling', 'websocket'],
    pingTimeout: 60000,
    pingInterval: 25000
});

// ═══════════════════════════════════════════════════════
// ★★★ Buffer مؤقت — يحفظ كل البيانات المرسلة بالترتيب ★★★
// ═══════════════════════════════════════════════════════
const DATA_BUFFER_MAX = 10000; // أقصى عدد رسائل محفوظة
let dataBuffer = [];           // [{seq, type, data, timestamp, acked_by:[]}]
let sequenceCounter = 0;       // عداد تسلسلي عام

// حالة مشتركة
let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', color: '---', ficha_id: '---' };

// ═══════════════════════════════════════════════════════
// ★ دالة إضافة رسالة إلى الـ Buffer
// ═══════════════════════════════════════════════════════
function addToBuffer(type, data, senderSocketId) {
    sequenceCounter++;
    const entry = {
        seq: sequenceCounter,
        type: type,
        data: data,
        timestamp: new Date().toISOString(),
        sender: senderSocketId,
        acked_by: []   // قائمة الأجهزة التي أكدت الاستلام
    };
    dataBuffer.push(entry);

    // تنظيف إذا تجاوز الحد
    if (dataBuffer.length > DATA_BUFFER_MAX) {
        const removeCount = dataBuffer.length - DATA_BUFFER_MAX;
        dataBuffer.splice(0, removeCount);
        console.log(`🧹 تنظيف Buffer: حذف ${removeCount} رسالة قديمة`);
    }

    return entry;
}

// ═══════════════════════════════════════════════════════
// ★ دالة جلب الرسائل بعد رقم تسلسل معين
// ═══════════════════════════════════════════════════════
function getBufferAfterSeq(afterSeq) {
    return dataBuffer.filter(entry => entry.seq > afterSeq);
}

// ═══════════════════════════════════════════════════════
// ★ دالة تسجيل تأكيد الاستلام
// ═══════════════════════════════════════════════════════
function markAcked(seq, socketId) {
    const entry = dataBuffer.find(e => e.seq === seq);
    if (entry && !entry.acked_by.includes(socketId)) {
        entry.acked_by.push(socketId);
    }
}

// ═══════════════════════════════════════════════════════
// ★ API endpoints
// ═══════════════════════════════════════════════════════
app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        clients: io.engine.clientsCount,
        machineStatus: machineStatus,
        lastData: lastData,
        bufferSize: dataBuffer.length,
        lastSeq: sequenceCounter,
        uptime: process.uptime()
    });
});

// ★ API للمزامنة عبر HTTP (بديل إذا كان Socket غير متاح)
app.get('/api/sync', (req, res) => {
    const afterSeq = parseInt(req.query.after_seq) || 0;
    const messages = getBufferAfterSeq(afterSeq);
    res.json({
        status: 'ok',
        last_seq: sequenceCounter,
        count: messages.length,
        messages: messages
    });
});

// ★ API لجلب حالة الـ Buffer
app.get('/api/buffer/status', (req, res) => {
    res.json({
        total_messages: dataBuffer.length,
        last_seq: sequenceCounter,
        machine_status: machineStatus,
        oldest_seq: dataBuffer.length > 0 ? dataBuffer[0].seq : 0,
        newest_seq: dataBuffer.length > 0 ? dataBuffer[dataBuffer.length - 1].seq : 0
    });
});

// ★ API لتأكيد الاستلام عبر HTTP
app.post('/api/ack', (req, res) => {
    const { seq, client_id } = req.body;
    if (seq && client_id) {
        markAcked(seq, client_id);
        res.json({ status: 'ok', seq: seq });
    } else {
        res.status(400).json({ status: 'error', message: 'seq and client_id required' });
    }
});

// ═══════════════════════════════════════════════════════
// ★★★ Socket.IO — مع Buffer + ACK ★★★
// ═══════════════════════════════════════════════════════
io.on('connection', (socket) => {
    console.log(`✅ متصل جديد: ${socket.id} | الإجمالي: ${io.engine.clientsCount}`);

    // ★ إرسال آخر حالة + رقم التسلسل الحالي
    socket.emit('welcome', {
        status: machineStatus,
        details: lastData,
        last_seq: sequenceCounter,
        buffer_size: dataBuffer.length
    });
    socket.emit('machine_status', { status: machineStatus });

    // ═══════════════════════════════════════════════════════
    // ★★★ طلب مزامنة — المستقبل يطلب البيانات المفقودة ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('sync_request', (payload) => {
        const afterSeq = payload.after_seq || 0;
        console.log(`🔄 [${socket.id}] طلب مزامنة بعد seq=${afterSeq}`);

        const missed = getBufferAfterSeq(afterSeq);
        console.log(`   📦 إرسال ${missed.length} رسالة فائتة`);

        // إرسال الرسائل المفقودة بالترتيب
        socket.emit('sync_response', {
            last_seq: sequenceCounter,
            count: missed.length,
            messages: missed,
            machine_status: machineStatus
        });
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ تأكيد الاستلام من المستقبل ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('ack', (payload) => {
        const seq = payload.seq;
        if (seq) {
            markAcked(seq, socket.id);
            // إرسال تأكيد للمرسل الأصلي
            const entry = dataBuffer.find(e => e.seq === seq);
            if (entry && entry.sender) {
                io.to(entry.sender).emit('delivery_confirmed', {
                    seq: seq,
                    acked_by: socket.id,
                    timestamp: new Date().toISOString()
                });
            }
        }
    });

    // ★ تأكيد استلام دفعة كاملة
    socket.on('ack_batch', (payload) => {
        const seqs = payload.seqs || [];
        seqs.forEach(seq => {
            markAcked(seq, socket.id);
        });
        console.log(`✅ [${socket.id}] تأكيد استلام ${seqs.length} رسالة`);

        // إبلاغ المرسلين
        seqs.forEach(seq => {
            const entry = dataBuffer.find(e => e.seq === seq);
            if (entry && entry.sender) {
                io.to(entry.sender).emit('delivery_confirmed', {
                    seq: seq,
                    acked_by: socket.id
                });
            }
        });
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ استقبال الأوامر — مع حفظ في Buffer ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('command', (payload) => {
        console.log(`📨 [${socket.id}] command:`, JSON.stringify(payload).substring(0, 200));

        const action = payload.action;
        const data = payload.data || payload.details || {};

        // ─── أوامر الآلة ───
        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            machineStatus = action;

            // ★ حفظ في Buffer
            const entry = addToBuffer('machine_command', {
                action: action,
                speed: data.speed || null,
                timestamp: payload.timestamp
            }, socket.id);

            // بث مع رقم التسلسل
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

            // ★ إرسال تأكيد للمرسل
            socket.emit('server_ack', {
                seq: entry.seq,
                original_action: action,
                status: 'buffered'
            });

            console.log(`   ⚙️ حالة الآلة: ${action} [seq=${entry.seq}]`);
        }

        // ─── ★★★ تغيير البيانات ★★★ ───
        if (action === 'data_change') {
            const changeType = payload.change_type;
            const details = payload.details || {};

            console.log(`   📡 نوع التغيير: ${changeType} [seq سيُنشأ]`);

            // تحديث البيانات المحلية
            if (details.tailor) lastData.tailor = details.tailor;
            if (details.embroidery) lastData.color = details.embroidery;
            if (details.ficha_id) lastData.ficha_id = details.ficha_id;

            // ★ حفظ في Buffer
            const entry = addToBuffer(changeType, details, socket.id);

            // ★ بث مع رقم التسلسل
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

            // ★ إرسال تأكيد للمرسل
            socket.emit('server_ack', {
                seq: entry.seq,
                original_action: changeType,
                status: 'buffered'
            });

            console.log(`   ✅ محفوظ في Buffer [seq=${entry.seq}]`);
        }

        // ─── طلب مزامنة ───
        if (action === 'SYNC_REQUEST') {
            const afterSeq = data.after_seq || 0;
            const missed = getBufferAfterSeq(afterSeq);

            socket.emit('sync_response', {
                last_seq: sequenceCounter,
                count: missed.length,
                messages: missed,
                machine_status: machineStatus
            });

            socket.emit('update_ui', {
                status: machineStatus,
                details: lastData,
                last_seq: sequenceCounter
            });

            console.log(`   🔄 مزامنة لـ ${socket.id}: ${missed.length} رسالة بعد seq=${afterSeq}`);
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ أحداث مباشرة — مع حفظ في Buffer ★★★
    // ═══════════════════════════════════════════════════════

    socket.on('ficha_saved', (data) => {
        console.log(`📋 [${socket.id}] ficha_saved`);

        const entry = addToBuffer('ficha_saved', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('ficha_saved', data);

        if (data.tailor) lastData.tailor = data.tailor;
        if (data.embroidery) lastData.color = data.embroidery;
        if (data.ficha_id) lastData.ficha_id = data.ficha_id;

        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });

        // ★ تأكيد للمرسل
        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'ficha_saved',
            status: 'buffered'
        });
    });

    socket.on('ficha_deleted', (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted`);

        const entry = addToBuffer('ficha_deleted', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('ficha_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'ficha_deleted',
            status: 'buffered'
        });
    });

    socket.on('production_saved', (data) => {
        console.log(`🧺 [${socket.id}] production_saved`);

        const entry = addToBuffer('production_saved', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('production_saved', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'production_saved',
            status: 'buffered'
        });
    });

    socket.on('production_deleted', (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted`);

        const entry = addToBuffer('production_deleted', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('production_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'production_deleted',
            status: 'buffered'
        });
    });

    socket.on('machine_status', (data) => {
        console.log(`⚙️ [${socket.id}] machine_status`);

        if (data.status) machineStatus = data.status;

        const entry = addToBuffer('machine_status', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('machine_status', data);
        io.emit('update_ui', { status: machineStatus, details: lastData, seq: entry.seq });

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'machine_status',
            status: 'buffered'
        });
    });

    socket.on('speed_update', (data) => {
        const entry = addToBuffer('speed_update', data, socket.id);
        data.seq = entry.seq;
        socket.broadcast.emit('speed_update', data);
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ بيانات تقنية مصفوفة (إحداثيات، أوامر رؤوس) ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('technical_data', (data) => {
        console.log(`🔧 [${socket.id}] technical_data: ${data.data_type || 'unknown'} — ${(data.points || []).length} نقطة`);

        const entry = addToBuffer('technical_data', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('technical_data', data);

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'technical_data',
            status: 'buffered',
            data_type: data.data_type
        });
    });

    // ★ بيانات إحداثيات دفعة واحدة
    socket.on('coordinates_batch', (data) => {
        console.log(`📍 [${socket.id}] coordinates_batch: ${(data.coordinates || []).length} إحداثية`);

        const entry = addToBuffer('coordinates_batch', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('coordinates_batch', data);

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'coordinates_batch',
            status: 'buffered'
        });
    });

    // ★ أمر رأس
    socket.on('head_command', (data) => {
        console.log(`🎯 [${socket.id}] head_command: ${data.command || 'unknown'}`);

        const entry = addToBuffer('head_command', data, socket.id);
        data.seq = entry.seq;

        socket.broadcast.emit('head_command', data);

        socket.emit('server_ack', {
            seq: entry.seq,
            original_action: 'head_command',
            status: 'buffered'
        });
    });

    socket.on('disconnect', () => {
        console.log(`❌ انقطع: ${socket.id} | المتبقي: ${io.engine.clientsCount}`);
    });
});

// ═══════════════════════════════════════════════════════
// ★ تنظيف دوري للـ Buffer القديم (كل 5 دقائق)
// ═══════════════════════════════════════════════════════
setInterval(() => {
    const now = new Date();
    const maxAge = 2 * 60 * 60 * 1000; // ساعتان
    const before = dataBuffer.length;
    dataBuffer = dataBuffer.filter(entry => {
        const age = now - new Date(entry.timestamp);
        return age < maxAge;
    });
    const removed = before - dataBuffer.length;
    if (removed > 0) {
        console.log(`🧹 تنظيف دوري: حذف ${removed} رسالة أقدم من ساعتين`);
    }
}, 5 * 60 * 1000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log('═'.repeat(65));
    console.log(`🚀 السيرفر جاهز على المنفذ ${PORT} — مع Buffer + ACK`);
    console.log(`📡 الأحداث المدعومة:`);
    console.log(`   command, ficha_saved, ficha_deleted`);
    console.log(`   production_saved, production_deleted`);
    console.log(`   machine_status, speed_update`);
    console.log(`   technical_data, coordinates_batch, head_command`);
    console.log(`   sync_request, ack, ack_batch`);
    console.log(`📊 Buffer: max ${DATA_BUFFER_MAX} رسالة | تنظيف كل 5 دقائق`);
    console.log('═'.repeat(65));
});