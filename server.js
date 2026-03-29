const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ['polling', 'websocket']
});

// ═══════════════════════════════════════════════════════
// حالة مشتركة (تُرسل لكل متصل جديد)
// ═══════════════════════════════════════════════════════
let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', color: '---', ficha_id: '---' };

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// ═══════════════════════════════════════════════════════
// نقطة صحة السيرفر (للتأكد أنه يعمل)
// ═══════════════════════════════════════════════════════
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        clients: io.engine.clientsCount,
        machineStatus: machineStatus,
        lastData: lastData,
        uptime: process.uptime()
    });
});

io.on('connection', (socket) => {
    console.log(`✅ متصل جديد: ${socket.id} | الإجمالي: ${io.engine.clientsCount}`);

    // ★ إرسال آخر حالة للمتصل الجديد فوراً
    socket.emit('update_ui', { status: machineStatus, details: lastData });
    socket.emit('machine_status', { status: machineStatus });

    // ═══════════════════════════════════════════════════════
    // ★★★ استقبال الأوامر وإعادة بثها بالأحداث الصحيحة ★★★
    // ═══════════════════════════════════════════════════════
    socket.on('command', (payload) => {
        console.log(`📨 [${socket.id}] command:`, JSON.stringify(payload));

        const action = payload.action;
        const data = payload.data || payload.details || {};

        // ─── أوامر الآلة ───
        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            machineStatus = action;
            // بث لجميع الأجهزة ما عدا المرسل
            socket.broadcast.emit('machine_status', {
                status: action,
                speed: data.speed || null,
                timestamp: payload.timestamp
            });
            // تحديث صفحة الويب
            io.emit('update_ui', {
                status: machineStatus,
                details: lastData
            });
            console.log(`   ⚙️ حالة الآلة: ${action}`);
        }

        // ─── ★★★ تغيير البيانات (الحدث المفقود!) ★★★ ───
        if (action === 'data_change') {
            const changeType = payload.change_type;
            const details = payload.details || {};

            console.log(`   📡 نوع التغيير: ${changeType}`);
            console.log(`   📦 التفاصيل:`, JSON.stringify(details));

            // تحديث البيانات المحلية للسيرفر
            if (details.tailor) lastData.tailor = details.tailor;
            if (details.embroidery) lastData.color = details.embroidery;
            if (details.ficha_id) lastData.ficha_id = details.ficha_id;

            // ★★★ إعادة البث بالحدث المطابق لما يستمع له المستقبل ★★★
            // بث لجميع الأجهزة ما عدا المرسل
            socket.broadcast.emit('data_changed', {
                type: changeType,
                change_type: changeType,
                details: details,
                timestamp: payload.timestamp
            });

            // تحديث صفحة الويب
            io.emit('update_ui', {
                status: machineStatus,
                details: lastData
            });
        }

        // ─── طلب مزامنة ───
        if (action === 'SYNC_REQUEST') {
            socket.emit('update_ui', {
                status: machineStatus,
                details: lastData
            });
            console.log(`   🔄 مزامنة لـ ${socket.id}`);
        }
    });

    // ═══════════════════════════════════════════════════════
    // ★★★ إعادة بث الأحداث المباشرة (ficha_saved, etc.) ★★★
    // ═══════════════════════════════════════════════════════

    socket.on('ficha_saved', (data) => {
        console.log(`📋 [${socket.id}] ficha_saved:`, JSON.stringify(data));
        // بث لجميع الأجهزة ما عدا المرسل
        socket.broadcast.emit('ficha_saved', data);
        // تحديث بيانات السيرفر
        if (data.tailor) lastData.tailor = data.tailor;
        if (data.embroidery) lastData.color = data.embroidery;
        if (data.ficha_id) lastData.ficha_id = data.ficha_id;
        io.emit('update_ui', { status: machineStatus, details: lastData });
    });

    socket.on('ficha_deleted', (data) => {
        console.log(`🗑️ [${socket.id}] ficha_deleted:`, JSON.stringify(data));
        socket.broadcast.emit('ficha_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData });
    });

    socket.on('production_saved', (data) => {
        console.log(`🧺 [${socket.id}] production_saved:`, JSON.stringify(data));
        socket.broadcast.emit('production_saved', data);
        io.emit('update_ui', { status: machineStatus, details: lastData });
    });

    socket.on('production_deleted', (data) => {
        console.log(`🗑️ [${socket.id}] production_deleted:`, JSON.stringify(data));
        socket.broadcast.emit('production_deleted', data);
        io.emit('update_ui', { status: machineStatus, details: lastData });
    });

    socket.on('machine_status', (data) => {
        console.log(`⚙️ [${socket.id}] machine_status:`, JSON.stringify(data));
        if (data.status) machineStatus = data.status;
        socket.broadcast.emit('machine_status', data);
        io.emit('update_ui', { status: machineStatus, details: lastData });
    });

    socket.on('speed_update', (data) => {
        console.log(`🏎️ [${socket.id}] speed_update:`, JSON.stringify(data));
        socket.broadcast.emit('speed_update', data);
    });

    socket.on('disconnect', () => {
        console.log(`❌ انقطع: ${socket.id} | المتبقي: ${io.engine.clientsCount}`);
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log('═'.repeat(60));
    console.log(`🚀 السيرفر جاهز على المنفذ ${PORT}`);
    console.log(`📡 الأحداث المدعومة:`);
    console.log(`   command, ficha_saved, ficha_deleted`);
    console.log(`   production_saved, production_deleted`);
    console.log(`   machine_status, speed_update`);
    console.log('═'.repeat(60));
});
