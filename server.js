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

let machineStatus = 'UNKNOWN';
let lastData = { tailor: '---', color: '---', ficha_id: '---' };

// جعل السيرفر يقرأ ملف index.html تلقائياً
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
    console.log(`✅ متصل جديد: ${socket.id}`);
    
    // إرسال آخر حالة مسجلة للمتصل الجديد فوراً
    socket.emit('update_ui', { status: machineStatus, details: lastData });

    // استقبال الأوامر من برنامج البايثون (جهازك)
    socket.on('command', (payload) => {
        console.log(`📨 وصلت بيانات:`, payload);

        if (payload.action === 'data_change' && payload.details) {
            lastData = payload.details;
        }
        if (['START', 'STOP', 'PAUSE'].includes(payload.action)) {
            machineStatus = payload.action;
        }

        // إرسال البيانات فوراً لجميع الشاشات المتصلة
        io.emit('update_ui', {
            status: machineStatus,
            details: lastData
        });
    });

    socket.on('disconnect', () => console.log(`❌ انقطع اتصال: ${socket.id}`));
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`🚀 السيرفر جاهز على المنفذ ${PORT}`));
