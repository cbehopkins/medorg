import asyncio

from sqlalchemy.ext.asyncio import create_async_engine


async def main() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin():
        pass
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())