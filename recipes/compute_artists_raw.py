import dataiku
import requests

API_KEY = dataiku.get_custom_variables()["LASTFM_API_KEY"]

resp = requests.get("https://ws.audioscrobbler.com/2.0/", params={
    "method":  "chart.getTopArtists",
    "api_key": API_KEY,
    "format":  "json",
    "limit":   3,
})

data = resp.json()
artists = data["artists"]["artist"]

for a in artists:
    print(f"{a['name']} — {int(a['listeners']):,} listeners")

print("\nAPI key works!")