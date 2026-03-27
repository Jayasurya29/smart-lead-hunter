import asyncio, os
from dotenv import load_dotenv
load_dotenv()
import httpx

async def test():
    key = os.getenv('APOLLO_API_KEY', '')
    print(f"Key: {key[:15]}..." if key else "NO KEY SET")
    async with httpx.AsyncClient() as c:
        r = await c.post(
            'https://api.apollo.io/api/v1/mixed_people/api_search',
            headers={'Content-Type': 'application/json', 'X-Api-Key': key},
            json={'q_organization_name': 'Marriott', 'person_locations': ['Miami, Florida'], 'person_titles': ['General Manager'], 'page': 1, 'per_page': 1}
        )
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            people = r.json().get('people', [])
            print(f"Found: {len(people)} people")
            if people:
                p = people[0]
                print(f"  {p.get('first_name')} {p.get('last_name')} - {p.get('title')}")
        else:
            print(f"Error: {r.text[:200]}")

asyncio.run(test())
