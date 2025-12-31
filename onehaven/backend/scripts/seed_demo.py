import asyncio
from app.services.demo_seed import seed_demo


async def main():
    await seed_demo()
    print("Seeded demo (idempotent) ✅")


if __name__ == "__main__":
    asyncio.run(main())
