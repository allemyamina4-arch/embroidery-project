// ═══════════════════════════════════════════════════════
// server.js — سيرفر التطريز على Render
// ═══════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    // ★ دعم polling + websocket معاً
    transports: ['polling', 'websocket']
});

// حالة الآلة الحالية
let machineStatus = 'UNKNOWN';

app.get('/', (req, res) => {
    const clientCount = io.engine.clientsCount;
    res.json({
        status: 'running',
        clients: clientCount,
        machineStatus: machineStatus,
        timestamp: new Date().toISOString()
    });
});

io.on('connection', (socket) => {
    console.log(`✅ عميل جديد متصل: ${socket.id} | النقل: ${socket.conn.transport.name}`);

    // إرسال حالة الآلة الحالية للعميل الجديد
    socket.emit('update_status', { status: machineStatus });

    // ═══════════════════════════════════════════════
    // ★★★ استقبال الأوامر وإعادة بثها ★★★
    // ═══════════════════════════════════════════════
    socket.on('command', (payload) => {
        console.log(`📨 أمر من ${socket.id}:`, JSON.stringify(payload));

        const action = payload.action;

        // أوامر التحكم بالآلة
        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            machineStatus = action;
            console.log(`⚙️ حالة الآلة: ${machineStatus}`);
            // ★ بث لكل العملاء بما فيهم المرسل
            io.emit('update_status', { status: machineStatus });
        }

        // تغيير بيانات (فيشة أو إنتاج)
        if (action === 'data_change') {
            console.log(`📡 تغيير بيانات: ${payload.change_type}`);

            // بث للجميع ما عدا المرسل
            socket.broadcast.emit('data_changed', {
                type: payload.change_type,
                details: payload.details,
                timestamp: payload.timestamp,
                from: socket.id
            });

            // إرسال حدث خاص حسب نوع التغيير
            if (payload.change_type === 'ficha_saved') {
                socket.broadcast.emit('ficha_saved', payload.details);
            } else if (payload.change_type === 'production_saved') {
                socket.broadcast.emit('production_saved', payload.details);
            }
        }

        // طلب تحديث
        if (action === 'SYNC_REQUEST') {
            console.log(`🔄 طلب تحديث من ${socket.id}`);
            socket.emit('update_status', {
                status: machineStatus,
                sync: true,
                timestamp: new Date().toISOString()
            });
        }
    });

    // ترقية النقل
    socket.conn.once('upgrade', () => {
        console.log(`⬆️ ترقية ${socket.id} إلى: ${socket.conn.transport.name}`);
    });

    socket.on('disconnect', (reason) => {
        console.log(`❌ عميل انقطع: ${socket.id} | السبب: ${reason}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 السيرفر يعمل على المنفذ ${PORT}`);
});