import os
import asyncio
import httpx

URL = "https://api.rentcast.io/v1/listings/sale?zipCode=48009&limit=1"

async def main():
    key = os.getenv("RENTCAST_API_KEY")
    print("Key set:", bool(key), "len:", len(key) if key else None)
    headers = {"accept": "application/json", "X-Api-Key": key or ""}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(URL, headers=headers)
        print("Status:", r.status_code)
        print("Body head:", r.text[:300])

asyncio.run(main())
