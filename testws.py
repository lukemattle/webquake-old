# opens a websocket and logs all messages to log.txt
import asyncio
import websockets

async def log_messages():
    uri = "ws://localhost:8000/ws"
    async with websockets.connect(uri) as websocket:
        async def send_ping():
            while True:
                await asyncio.sleep(10)
                await websocket.send("!ping")
        asyncio.create_task(send_ping())
        f = open('log.txt', 'w')
        f.close()
        async for message in websocket:
            if message != '!ping' or message != '!pong':
                f = open('log.txt', 'a', encoding='utf-8')
                f.write(str(message) + '\n')
                f.close()

asyncio.run(log_messages())