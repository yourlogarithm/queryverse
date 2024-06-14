import aiohttp
import asyncio
import aio_pika
import aio_pika.abc
from edges_pb2 import EdgesMessage
from urllib.parse import quote


async def main():
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )
    async with connection:
        queue_name = "crawler"
        channel = await connection.channel()
        queue = await channel.get_queue(queue_name)
        async with queue.iterator() as queue_iter:
            async with aiohttp.ClientSession() as session:
                async for message in queue_iter:
                    async with message.process():
                        msg = EdgesMessage()
                        msg.ParseFromString(message.body)
                        for url in msg.urls:
                            url = 'http://localhost:8000/v1/crawl/' + quote(url, safe='')
                            async with session.get(url) as response:
                                print(f"{response.status} - {url}")

if __name__ == "__main__":
    asyncio.run(main())
