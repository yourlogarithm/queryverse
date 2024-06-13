import aiohttp
import asyncio
import aio_pika
import aio_pika.abc
from urllib.parse import quote_from_bytes


async def main():
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )
    async with connection:
        queue_name = "crawled_urls"
        channel = await connection.channel()
        queue = await channel.get_queue(queue_name)
        async with queue.iterator() as queue_iter:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async for message in queue_iter:
                    async with message.process():
                        to_crawl = quote_from_bytes(message.body, safe='')
                        async with session.get('https://localhost:8000/v1/url/' + quote_from_bytes(message.body)) as response:
                            print(f"{response.status} - {to_crawl}")

if __name__ == "__main__":
    asyncio.run(main())
