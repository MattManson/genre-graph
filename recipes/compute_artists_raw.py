import dataiku
import requests
import time
import pandas as pd
from datetime import datetime

# Get API key from project variables
project = dataiku.Project()
variables = project.get_variables()
api_key = "e4dfce9570521e545ee28165fbe85a82"

BASE_URL = "http://ws.audioscrobbler.com/2.0/"

# Manual seed tags for underground/niche scenes
MANUAL_SEED_TAGS = [
    "sludge metal", "blackgaze", "post-hardcore", "powerviolence",
    "screamo", "mathcore", "crust punk", "d-beat", "holy terror",
    "post-metal", "drone metal", "doom metal", "stoner metal",
    "black metal", "death metal", "grindcore", "metalcore",
    "atmospheric black metal", "depressive black metal", "shoegaze",
    "post-rock", "noise rock", "hardcore punk", "melodic hardcore",
    "skramz", "emo violence", "midwest emo", "surf punk", "post-punk",
    "dark hardcore", "beatdown hardcore", "progressive metal",
    "technical death metal", "brutal death metal", "funeral doom"
]

def api_call(params, retries=3):
    """Make a rate-limited API call with retry logic"""
    params.update({
        "api_key": api_key,
        "format": "json"
    })
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                print(f"API error: {data.get('message', 'unknown')}")
                return None
            return data
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    return None

def get_top_global_tags(limit=500):
    """Get top tags globally from Last.fm"""
    data = api_call({"method": "tag.getTopTags"})
    if not data:
        return []
    tags = data.get("toptags", {}).get("tag", [])
    return [t["name"] for t in tags[:limit]]

def get_artists_for_tag(tag, limit=50):
    """Get top artists for a given tag"""
    data = api_call({
        "method": "tag.gettopartists",
        "tag": tag,
        "limit": limit
    })
    if not data:
        return []
    artists = data.get("topartists", {}).get("artist", [])
    return [a["name"] for a in artists if isinstance(a, dict)]

def get_tags_for_artist(artist):
    """Get top tags for a given artist"""
    data = api_call({
        "method": "artist.getTopTags",
        "artist": artist
    })
    if not data:
        return []
    tags = data.get("toptags", {}).get("tag", [])
    return [(t["name"], int(t["count"])) for t in tags if isinstance(t, dict)]

# Step 1: Build full tag list
print("Fetching global top tags...")
global_tags = get_top_global_tags(500)
all_tags = list(set(global_tags + MANUAL_SEED_TAGS))
print(f"Total tags to process: {len(all_tags)}")

# Step 2: Get artists for each tag
print("Fetching artists for each tag...")
artist_set = set()
for i, tag in enumerate(all_tags):
    if i % 50 == 0:
        print(f"Processing tag {i}/{len(all_tags)}: {tag}")
    artists = get_artists_for_tag(tag)
    artist_set.update(artists)
    time.sleep(0.2)  # 5 requests per second max

print(f"Unique artists found: {len(artist_set)}")

# Step 3: Get tags for each artist and build flat table
print("Fetching tags for each artist...")
rows = []
ingested_at = datetime.utcnow().isoformat()

for i, artist in enumerate(artist_set):
    if i % 100 == 0:
        print(f"Processing artist {i}/{len(artist_set)}: {artist}")
    artist_tags = get_tags_for_artist(artist)
    for tag_name, tag_count in artist_tags:
        rows.append({
            "artist_name": artist,
            "tag_name": tag_name.lower().strip(),
            "tag_count": tag_count,
            "ingested_at": ingested_at
        })
    time.sleep(0.2)

# Step 4: Write to Snowflake via DSS
print(f"Writing {len(rows)} rows to artists_raw...")
df = pd.DataFrame(rows)

output = dataiku.Dataset("artists_raw")
output.write_with_schema(df)

print("Done.")