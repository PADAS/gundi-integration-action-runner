import asyncio

import httpx
from urllib.parse import urljoin


async def get_post(*, topic:dict, post_url:str, username:str, apikey: str):

    headers = {'Api-key': apikey
                , 'Api-Username': username
                , 'Content-Type': 'application/json'}
    
    async with httpx.AsyncClient() as client:
        response = await client.get(post_url, headers=headers)
        if response.is_success:
            post = response.json()
            return (topic, post)


async def get_feed_topics(*, topics_url:str, username:str, apikey: str):
    
    headers = {'Api-key': apikey, 'Api-Username': username, 'Content-Type': 'application/json'}

    async with httpx.AsyncClient() as client:
        response = await client.get(topics_url, headers=headers)
        if response.is_success:
            latest_feed = response.json()
            return latest_feed
        

async def get_topics_per_tag(*, topics_url:str, username:str, apikey: str):

    feed_data = await get_feed_topics(topics_url=topics_url, username=username, apikey=apikey)

    if feed_data:

        async with httpx.AsyncClient() as client:
            all_topics = feed_data['topic_list']['topics']

            filtered_list = []

            topics = [topic for topic in all_topics if 'er-notify' in topic.get('tags', [])]

            tasks = [get_post(topic=topic, post_url=urljoin(topics_url, f'/t/{topic["id"]}/posts.json'), username=username, apikey=apikey) for topic in topics]
            results = await asyncio.gather(*tasks)

            for topic, item in results:

                post = item['post_stream']['posts'][0]
                if 'cooked' in post:
                    topic['cooked'] = post['cooked']
                    filtered_list.append(topic)
        
            feed_data['topic_list']['topics'] = filtered_list

            return feed_data
