import requests
import pandas as pd
import os
import pytz
import time
from datetime import datetime, timedelta

def get_steam_data():
    """
    调用 GetOwnedGames 和 GetRecentlyPlayedGames 接口获取游戏时长数据
    将结果保存为 CSV 文件
    """
    all_url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1"
    recently_url = (
        "https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1"
    )

    # 将 key 和 steamid 配置在代码仓库的 Actions secrets and variables 中,Actions 执行时将其赋值给环境变量
    key = os.environ.get("STEAM_KEY")
    steamid = os.environ.get("STEAM_ID")

    if not key or not steamid:
        raise ValueError("STEAM_KEY or STEAM_ID is not set in environment variables")

    params = {
        "key": key,
        "steamid": steamid,
        "include_appinfo": True,
        "include_played_free_games": True,
        "include_free_sub": True,
    }

    try:
        # 获取所有游戏信息
        all_response = requests.get(all_url, params=params)
        all_response.raise_for_status()
        all_res = all_response.json().get("response", {}).get("games")
        if not all_res:
            raise ValueError("Failed to get owned games data from Steam API")
        all_steam_df = pd.DataFrame(all_res)

        now_time = datetime.now(pytz.timezone("Asia/Shanghai"))
        all_steam_df["creation_time"] = now_time

        if "rtime_last_played" in all_steam_df.columns:
            all_steam_df["rtime_last_played"] = all_steam_df["rtime_last_played"].astype(int)
        if "playtime_disconnected" in all_steam_df.columns:
            all_steam_df["playtime_disconnected"] = all_steam_df["playtime_disconnected"].astype(int)
        if "playtime_2weeks" in all_steam_df.columns:
            all_steam_df["playtime_2weeks"] = all_steam_df["playtime_2weeks"].fillna(0).astype(int)

        os.makedirs("./data/steam_data", exist_ok=True)
        all_steam_df.to_csv(f"./data/steam_data/steam_data_{now_time.strftime('%Y%m%d')}.csv", index=False)

        # 获取两周内游戏信息
        recently_response = requests.get(recently_url, params=params)
        recently_response.raise_for_status()
        recently_res = recently_response.json().get("response", {}).get("games")
        if not recently_res:
            print("Warning: No recently played games data from Steam API")
            steam_df = pd.DataFrame()
        else:
            steam_df = pd.DataFrame(recently_res)
        
        steam_df["created_time"] = now_time
        os.makedirs("./data/playtime_2week_data", exist_ok=True)
        steam_df.to_csv(f"./data/playtime_2week_data/steam_playtime_2week_{now_time.strftime('%Y%m%d')}.csv", index=False)

    except requests.RequestException as e:
        print(f"Network or API request error: {str(e)}")
    except Exception as e:
        print(f"An error occurred in get_steam_data: {str(e)}")

def merge_steam_data():
    """
    合并所有游戏数据与两周内游戏数据
    """
    try:
        today_date = datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y%m%d")
        all_game_file = f"./data/steam_data/steam_data_{today_date}.csv"
        if not os.path.exists(all_game_file):
            print(f"Warning: All games data file not found: {all_game_file}")
            return

        all_game_info = pd.read_csv(all_game_file)

        recently_file = f"./data/playtime_2week_data/steam_playtime_2week_{today_date}.csv"
        if not os.path.exists(recently_file):
            print("Warning: Recently played games data file not found, skipping merge")
            return

        recently_game_info = pd.read_csv(recently_file)

        for df in [all_game_info, recently_game_info]:
            if 'name' not in df.columns:
                df['name'] = 'Unknown Game'
            if 'appid' not in df.columns:
                print("Error: 'appid' column missing, cannot merge")
                return

        not_in_all_game_info = recently_game_info[~recently_game_info["appid"].isin(all_game_info["appid"])]

        now_time = datetime.now(pytz.timezone("Asia/Shanghai"))
        for index, row in not_in_all_game_info.iterrows():
            new_row = {
                "appid": row["appid"],
                "name": row["name"],
                "playtime_forever": row["playtime_forever"] if "playtime_forever" in row else 0,
                "playtime_windows_forever": row["playtime_windows_forever"] if "playtime_windows_forever" in row else 0,
                "playtime_mac_forever": row["playtime_mac_forever"] if "playtime_mac_forever" in row else 0,
                "playtime_linux_forever": row["playtime_linux_forever"] if "playtime_linux_forever" in row else 0,
                "playtime_deck_forever": row["playtime_deck_forever"] if "playtime_deck_forever" in row else 0,
                "playtime_2weeks": row["playtime_2weeks"] if "playtime_2weeks" in row else 0,
                "creation_time": now_time,
            }
            all_game_info = pd.concat([all_game_info, pd.DataFrame([new_row])], ignore_index=True)

        if "rtime_last_played" in all_game_info.columns:
            all_game_info["rtime_last_played"] = all_game_info["rtime_last_played"].fillna(0).astype(int)
        if "playtime_disconnected" in all_game_info.columns:
            all_game_info["playtime_disconnected"] = all_game_info["playtime_disconnected"].fillna(0).astype(int)

        all_game_info.to_csv(f"./data/steam_data/steam_data_{today_date}.csv", index=False)
        print(f"成功合并游戏数据: steam_data_{today_date}.csv")

    except Exception as e:
        print(f"在 merge_steam_data 中发生错误: {str(e)}")

def upload_to_notion():
    """
    将最近一天的游戏时长数据上传到Notion数据库
    """
    try:
        # 获取昨天的日期（数据日期）
        data_date = (datetime.now(pytz.timezone("Asia/Shanghai")) - timedelta(1)).strftime("%Y%m%d")
        csv_path = f"./data/playing_time_data/playing_time_{data_date}.csv"

        if not os.path.exists(csv_path):
            print(f"数据文件不存在: {csv_path}")
            return

        df = pd.read_csv(csv_path)
        if df.empty:
            print("没有需要上传的数据")
            return

        notion_key = os.environ.get("NOTION_KEY")
        notion_database_id = os.environ.get("NOTION_DATABASE_ID")

        if not notion_key or not notion_database_id:
            raise ValueError("NOTION_KEY 或 NOTION_DATABASE_ID 未设置")

        headers = {
            "Authorization": f"Bearer {notion_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }

        df["playtime_date"] = pd.to_datetime(df["playtime_date"]).dt.strftime("%Y-%m-%d")

        for _, row 在 df.iterrows():
            game_name = row["name"] if "name" in row and pd.notna(row["name"]) else "Unknown Game"
            page_properties = {
                "parent": {"database_id": notion_database_id},
                "properties": {
                    "游戏名称": {
                        "title": [
                            {
                                "text": {
                                    "content": game_name
                                }
                            }
                        ]
                    }，
                    "游戏ID": {
                        "number": int(row["appid"]) if "appid" in row else 0
                    }，
                    "游戏时长(分钟)": {
                        "number": int(row["playing_time"]) if "playing_time" in row else 0
                    },
                    "日期": {
                        "date": {
                            "start": row["playtime_date"] if "playtime_date" in row else "1970-01-01"
                        }
                    }
                }
            }

            response = requests.post(
                "https://api.notion.com/v1/pages",
                headers=headers,
                json=page_properties
            )

            if response.status_code == 200:
                print(f"成功上传: {game_name} - {int(row.get('playing_time', 0))}分钟")
            elif response.status_code == 429:
                print("达到API速率限制,暂停5秒...")
                time.sleep(5)
                response = requests.post(
                    "https://api.notion.com/v1/pages",
                    headers=headers,
                    json=page_properties
                )
                if response.status_code == 200:
                    print(f"重试成功: {game_name} - {int(row.get('playing_time', 0))}分钟")
                else:
                    print(f"重试失败: {response.status_code} - {response.text}")
            else:
                print(f"上传失败: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"在 upload_to_notion 中发生错误: {str(e)}")

# 主函数
if __name__ == "__main__":
    get_steam_data()
    merge_steam_data()
    upload_to_notion()
