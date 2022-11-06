from datetime import datetime as dt
from mpire import WorkerPool
from vk_api import VkApi
from math import ceil
import json as j
import re
import os

token = os.environ.get('VK_TOKEN')
forg_id = int(os.environ.get('GROUP_ID'))
vk = VkApi(token=token)
items = 200


def get_messages(offset):
    request_data = {
        "chat_id": forg_id,
        'count': items,
        'offset': offset * items
    }
    return vk.method("messages.getHistory", request_data)['items']


def get_lvl(food, lvl):
    return sum(list(range(11, int(lvl))))+int(food)+5 if int(lvl) > 10 else max(int(food)-5, 0)


def get_stats(s, d=0):
    r = re.search(
        '.*Имя( вашей)? жабы: (.+)\n.*\n.*Сытость: (\d+)\/(\d+)(\n?.*)*', s)
    return {
        "name": r.group(2),
        "date": dt.fromtimestamp(d).date(),
        "lvl": get_lvl(r.group(3), r.group(4))
    } if r else 0


def concat_list(arr): return [item for sublist in arr for item in sublist]


if __name__ == '__main__':
    count = vk.method("messages.getHistory", {"chat_id": forg_id})['count']
    api_calls_count = ceil(count / items)
    print(count, api_calls_count)

    with WorkerPool() as pool:
        results = pool.map(
            get_messages,
            range(api_calls_count),
            progress_bar=True)

    forgs = concat_list(results)[::-1]
    forgs.sort(key=lambda x: x['date'])  # sort by timestamp

    forgs_data = [
        get_stats(m["text"], m["date"]) for m in forgs
        if get_stats(m["text"])
    ]
    print(len(forgs), len(forgs_data))

    forg_names = list(set([f["name"] for f in forgs_data]))
    print(len(forg_names), forg_names)

    def lv(name, lvl): return lvl if name else 0

    data = [
        {
            "date": i["date"].strftime('%d-%m-%y'),
            **{k: lv(i["name"] == k, i["lvl"]) for k in forg_names}
        } for i in forgs_data
    ]

    for name in forg_names:
        max_lvl = 0
        for i, t in list(enumerate(data))[1:]:
            t[name] = max(t[name], data[i - 1][name])

    fdata = []

    for i in data[::-1]:
        if not i["date"] in [i["date"] for i in fdata]:
            fdata.append(i)

    fdata = fdata[::-1]

    print(len(data), len(fdata), len(data) / len(fdata))

    forgs_fdata = [
        {
            "name": k,
            **{t: [i for i in fdata if i["date"] == t][0][k] for t in [i["date"] for i in fdata]}
        } for k in forg_names
    ]

    with open("forgs.json", "w") as f:
        f.write(j.dumps(forgs_fdata))
