import requests
import pandas as pd
import os
from datetime import datetime
import pytz
import json

def get_steam_data():
    """
    调用 GetOwnedGames 和 GetRecentlyPlayedGames 接口获取游戏时长数据
    将结果保存为 CSV 文件
    """

    all_url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1"
    recently_url = (
        "https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1"
    )

    key = os.environ.get("STEAM_KEY")
    steamid = os.environ.get("STEAM_ID")

    if key is None or steamid is None:
        raise ValueError("STEAM_KEY or STEAM_ID is not set")

    params = {
        "key": key,
        "steamid": steamid,
        "include_appinfo": True,
        "include_played_free_games": True,
        "include_free_sub": True,
    }

    # 获取当前时间，转换为北京时间
    now_time = datetime.now(pytz.timezone("Asia/Shanghai"))
    all_response = requests.request("GET", all_url, params=params)
    all_res = all_response.json().get("response").get("games")
    all_steam_df = pd.DataFrame(all_res)

    # 转换时间字段为北京时间
    all_steam_df['rtime_last_played'] = pd.to_datetime(all_steam_df['rtime_last_played'], unit='s')
    all_steam_df['rtime_last_played'] = all_steam_df['rtime_last_played'].dt.tz_localize('US/Pacific').dt.tz_convert('Asia/Shanghai')

    all_steam_df['playtime_2weeks'] = (
        all_steam_df["playtime_2weeks"].fillna(0).astype(int)
    )

    os.makedirs("./data/steam_data", exist_ok=True)
    all_steam_df.to_csv(
        f"./data/steam_data/steam_data_{now_time.strftime('%Y%m%d')}.csv", index=False
    )

    # 获取两周内游戏信息
    recently_response = requests.request("GET", recently_url, params=params)
    recently_res = recently_response.json().get("response").get("games")
    steam_df = pd.DataFrame(recently_res)

    # 转换时间为北京时间
    steam_df['created_time'] = pd.to_datetime(steam_df['created_time'], unit='s')
    steam_df['created_time'] = steam_df['created_time'].dt.tz_localize('US/Pacific').dt.tz_convert('Asia/Shanghai')

    steam_df["created_time"] = now_time
    os.makedirs("./data/playtime_2week_data", exist_ok=True)
    steam_df.to_csv(
        f"./data/playtime_2week_data/steam_playtime_2week_{now_time.strftime('%Y%m%d')}.csv",
        index=False,
    )


def merge_steam_data():
    """
    合并所有游戏数据与两周内游戏数据
    """
    today_date = datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y%m%d")
    all_game_info = pd.read_csv(f"./data/steam_data/steam_data_{today_date}.csv")
    recently_game_info = pd.read_csv(f"./data/playtime_2week_data/steam_playtime_2week_{today_date}.csv")

    not_in_all_game_info = recently_game_info[~recently_game_info["appid"].isin(all_game_info["appid"])]
    now_time = datetime.now(pytz.timezone("Asia/Shanghai"))

    for _, row in not_in_all_game_info.iterrows():
        new_row = {
            "appid": row["appid"],
            "name": row["name"],
            "playtime_forever": row["playtime_forever"],
            "playtime_windows_forever": row["playtime_windows_forever"],
            "playtime_mac_forever": row["playtime_mac_forever"],
            "playtime_linux_forever": row["playtime_linux_forever"],
            "playtime_deck_forever": row["playtime_deck_forever"],
            "playtime_2weeks": row["playtime_2weeks"],
            "creation_time": now_time,
        }
        all_game_info = pd.concat([all_game_info, pd.DataFrame([new_row])], ignore_index=True)

    all_game_info["rtime_last_played"] = all_game_info["rtime_last_played"].fillna(0).astype(int)
    all_game_info["playtime_disconnected"] = all_game_info["playtime_disconnected"].fillna(0).astype(int)
    all_game_info.to_csv(f"./data/steam_data/steam_data_{today_date}.csv", index=False)


def get_playing_time():
    """
    获取最近一天的游戏时长信息
    """
    today_date = datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y%m%d")
    yesterday_date = (datetime.now(pytz.timezone("Asia/Shanghai")) - timedelta(1)).strftime("%Y%m%d")

    today_game_info = pd.read_csv(f"./data/steam_data/steam_data_{today_date}.csv")
    yesterday_game_info = pd.read_csv(f"./data/steam_data/steam_data_{yesterday_date}.csv")

    merge_game_info = pd.merge(today_game_info, yesterday_game_info, on="appid", how="left", suffixes=("_new", "_old"))

    merge_game_info["playing_time"] = merge_game_info.apply(
        lambda x: x["playtime_forever_new"] + x["playtime_disconnected_new"] if pd.isna(x["playtime_forever_old"])
        else int(x["playtime_forever_new"] + x["playtime_disconnected_new"] - x["playtime_forever_old"] - x["playtime_disconnected_old"]),
        axis=1
    )

    merge_game_info.drop("name_old", axis=1, inplace=True)
    merge_game_info.rename(columns={"name_new": "name"}, inplace=True)

    merge_game_info = merge_game_info[merge_game_info["playing_time"] > 0]
    merge_game_info = merge_game_info[["appid", "name", "playing_time"]]
    merge_game_info["playtime_date"] = (datetime.now(pytz.timezone("Asia/Shanghai")) - timedelta(1)).date()
    merge_game_info["creation_time"] = datetime.now(pytz.timezone("Asia/Shanghai"))

    os.makedirs("./data/playing_time_data", exist_ok=True)
    merge_game_info.to_csv(f"./data/playing_time_data/playing_time_{today_date}.csv", index=False)


NOTION_TOKEN = os.environ.get("NOTION_KEY")
DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
DATA_DIR = "./data/playing_time_data"


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2021-05-13"
    }


def add_to_notion(data):
    url = f"https://api.notion.com/v1/pages"
    for _, row in data.iterrows():
        notion_payload = {
            "parent": { "database_id": DATABASE_ID },
            "properties": {
                "Game Name": {
                    "title": [
                        {
                            "text": {
                                "content": row['name']
                            }
                        }
                    ]
                },
                "Playtime": {  # 确保这里是 number 类型
                    "number": row['playing_time']
                },
                "Playtime Date": {
                    "date": {
                        "start": str(row['playtime_date'])
                    }
                },
                "AppID": {  # 确保这里是正确的字段类型（rich_text 或 number）
                    "rich_text": [
                        {
                            "text": {
                                "content": str(row['appid'])
                            }
                        }
                    ]
                }
            }
        }

        response = requests.post(url, headers=notion_headers(), data=json.dumps(notion_payload))

        if response.status_code == 200:
            print(f"Successfully added: {row['name']}")
        else:
            print(f"Failed to add: {row['name']} - {response.status_code} - {response.text}")

def get_playtime_data():
    all_files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.startswith("playing_time_") and f.endswith(".csv")
    ]
    if not all_files:
        print("No data files found.")
        return

    df_list = [pd.read_csv(f) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    df["playtime_date"] = pd.to_datetime(df["playtime_date"]).dt.strftime("%Y-%m-%d")

    return df


def main():
    data = get_playtime_data()
    if data is not None:
        add_to_notion(data)


if __name__ == "__main__":
    get_steam_data()
    merge_steam_data()
    get_playing_time()
    main()


