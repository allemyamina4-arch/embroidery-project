// ═══════════════════════════════════════════════════════
// server.js — سيرفر التطريز المطور لـ Render
// ═══════════════════════════════════════════════════════
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*", // يسمح بالاتصال من أي مصدر (برنامج البايثون أو المتصفح)
        methods: ["GET", "POST"]
    },
    transports: ['polling', 'websocket']
});

// متغيرات لتخزين حالة الآلة وآخر بيانات مستلمة
let machineStatus = 'UNKNOWN';
let lastData = {}; 

// صفحة الاختبار الرئيسية (تظهر عند فتح الرابط في المتصفح)
app.get('/', (req, res) => {
    res.json({
        status: 'Online',
        machineStatus: machineStatus,
        lastData: lastData,
        clientsCount: io.engine.clientsCount,
        timestamp: new Date().toISOString()
    });
});

io.on('connection', (socket) => {
    console.log(`✅ متصل جديد: ${socket.id}`);

    // إرسال الحالة الحالية فور دخول أي مستخدم جديد
    socket.emit('update_status', { status: machineStatus, details: lastData });

    // ═══════════════════════════════════════════════
    // استقبال البيانات من برنامج البايثون وإعادة بثها
    // ═══════════════════════════════════════════════
    socket.on('command', (payload) => {
        console.log(`📨 استلام بيانات:`, payload);

        const action = payload.action;

        // 1. تحديث حالة التشغيل (START/STOP/PAUSE)
        if (['START', 'STOP', 'PAUSE'].includes(action)) {
            machineStatus = action;
        }

        // 2. معالجة بيانات الإنتاج والفيشة (data_change)
        if (action === 'data_change') {
            lastData = payload.details; // حفظ آخر بيانات (اسم الخياط، اللون، إلخ)
            
            // ★ الأهم: إرسال البيانات فوراً لكل المتصفحات المتصلة ★
            io.emit('data_changed', {
                type: payload.change_type,
                details: payload.details,
                machineStatus: machineStatus,
                timestamp: new Date().toISOString()
            });
        } else {
            // في حال كانت الأوامر تشغيل/إيقاف فقط، نحدث الحالة للجميع
            io.emit('update_status', { status: machineStatus, details: lastData });
        }
    });

    socket.on('disconnect', (reason) => {
        console.log(`❌ انقطع الاتصال: ${socket.id} | السبب: ${reason}`);
    });
});

// المنفذ الخاص بـ Render (10000 أو المنفذ الافتراضي 3000)
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 السيرفر يعمل بنجاح على المنفذ ${PORT}`);
});
