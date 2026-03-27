from flask import Flask
from flask_socketio import SocketIO, emit

app = Flask(__name__)
# هذا المحرك يسمح بالاتصال من أي مكان في العالم
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def home():
    return "سيرفر خالد يعمل أونلاين!"

# عندما يرسل أي مستخدم (A) بيانات، السيرفر يرسلها فوراً للبقية (B, C)
@socketio.on('send_update')
def handle_message(data):
    print("بيانات مستلمة:", data)
    emit('receive_update', data, broadcast=True)

if __name__ == "__main__":
    socketio.run(app)