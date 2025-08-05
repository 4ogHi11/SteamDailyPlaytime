import requests
import pandas as pd
import os
import datetime
import pytz
from datetime import datetime, timedelta  # 确保导入timedelta

def get_steam_data():
    """
    调用 GetOwnedGames 和 GetRecentlyPlayedGames 接口获取游戏时长数据
    将结果保存为 CSV 文件
    """

    all_url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1"
    recently_url = (
        "https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1"
    )

    # 将 key 和 steamid 配置在代码仓库的 Actions secrets and variables 中，Actions 执行时将其赋值给环境变量
    key = os.environ.get("STEAM_KEY")
    steamid = os.environ.get("STEAM_ID")

    # 检查 key 和 steamid 是否设置
    if key is None or steamid is None:
        raise ValueError("STEAM_KEY or STEAM_ID is not set")

    params = {
        "key": key,
        "steamid": steamid,
        # 是否包含游戏信息，该参数为 True 时会返回游戏名称
        "include_appinfo": True,
        # 是否包含免费游戏信息，有两个参数，不确定这两个之间的区别
        "include_played_free_games": True,
        "include_free_sub": True,
    }

    # 获取所有游戏信息
    all_response = requests.request("GET", all_url, params=params)
    all_res = all_response.json().get("response").get("games")
    all_steam_df = pd.DataFrame(all_res)

    # 获取当前时间，由于 GitHub Actions 使用的是UTC 时间，因此需要转换为北京时间，后续所有时间相关内容均同
    now_time = datetime.datetime.now(pytz.timezone("Asia/Shanghai"))

    all_steam_df["creation_time"] = now_time

    # 个人强迫症，避免数据转换过程中出现浮点数导致出现无意义的.0
    all_steam_df["rtime_last_played"] = all_steam_df["rtime_last_played"].astype(int)
    all_steam_df["playtime_disconnected"] = all_steam_df[
        "playtime_disconnected"
    ].astype(int)
    # 此处由于该接口返回数据两周内游戏市场为 0 时会为空，因此需要将空值填充为 0，也避免转换失败
    all_steam_df["playtime_2weeks"] = (
        all_steam_df["playtime_2weeks"].fillna(0).astype(int)
    )
    # 创建文件夹，其实只需要初次创建后即可删除
    os.makedirs("./data/steam_data", exist_ok=True)
    # 保存数据，文件名记录日期
    all_steam_df.to_csv(
        f"./data/steam_data/steam_data_{now_time.strftime('%Y%m%d')}.csv", index=False
    )

    # 获取两周内游戏信息
    recently_response = requests.request("GET", recently_url, params=params)
    recently_res = recently_response.json().get("response").get("games")
    steam_df = pd.DataFrame(recently_res)
    steam_df["created_time"] = now_time
    os.makedirs("./data/playtime_2week_data", exist_ok=True)
    steam_df.to_csv(
        f"./data/playtime_2week_data/steam_playtime_2week_{now_time.strftime('%Y%m%d')}.csv",
        index=False,
    )


def merge_steam_data():
    """
    合并所有游戏数据与两周内游戏数据，由于 Steam 的家庭共享功能，
    获取所有游戏数据时无法获取家庭共享游戏数据，因此需要合并数据
    """

    # 获取当天的日期，以获取准确的文件名
    today_date = datetime.datetime.now(pytz.timezone("Asia/Shanghai")).strftime(
        "%Y%m%d"
    )
    # 从 CSV 文件中读取数据
    all_game_info = pd.read_csv(f"./data/steam_data/steam_data_{today_date}.csv")
    recently_game_info = pd.read_csv(
        f"./data/playtime_2week_data/steam_playtime_2week_{today_date}.csv"
    )

    # 获取不在 all_game_info 中的数据
    not_in_all_game_info = recently_game_info[
        ~recently_game_info["appid"].isin(all_game_info["appid"])
    ]
    # 使用两周内游戏数据补充数据
    now_time = datetime.datetime.now(pytz.timezone("Asia/Shanghai"))
    for index, row in not_in_all_game_info.iterrows():
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
        all_game_info = pd.concat(
            [all_game_info, pd.DataFrame([new_row])], ignore_index=True
        )
    # 此处合并数据后部分字段出现浮点数，因此转换为整数
    all_game_info["rtime_last_played"] = (
        all_game_info["rtime_last_played"].fillna(0).astype(int)
    )
    all_game_info["playtime_disconnected"] = (
        all_game_info["playtime_disconnected"].fillna(0).astype(int)
    )
    # 保存数据
    all_game_info.to_csv(f"./data/steam_data/steam_data_{today_date}.csv", index=False)


def get_playing_time():
    """
    获取最近一天的游戏时长信息
    """

    # 获取当天的日期，以获取准确的文件名
    today_date = datetime.datetime.now(pytz.timezone("Asia/Shanghai")).strftime(
        "%Y%m%d"
    )
    # 获取昨天的日期
    昨天_date = (
        datetime.datetime.now(pytz.timezone("Asia/Shanghai")) - datetime.timedelta(1)
    ).strftime("%Y%m%d")

    # 从 CSV 文件中读取数据
    today_game_info = pd.read_csv(f"./data/steam_data/steam_data_{today_date}.csv")
    昨天_game_info = pd.read_csv(
        f"./data/steam_data/steam_data_{yesterday_date}.csv"
    )

    # 计算最近一天的游戏时长
    # 针对每个 appid，计算当天游戏时长与昨天游戏时长的差值
    # 进行 merge 操作，使用 left join，因为今天可能存在昨天没有的游戏信息（刚购买）
    merge_game_info = pd.merge(
        today_game_info,
        yesterday_game_info,
        on="appid",
        how="left",
        suffixes=("_new", "_old"),
    )
    # 计算游戏时长，需要加上 disconnected 的时长
    merge_game_info["playing_time"] = merge_game_info.apply(
        lambda x: x["playtime_forever_new"] + x["playtime_disconnected_new"]
        if pd.isna(x["playtime_forever_old"])
        else int(
            x["playtime_forever_new"]
            + x["playtime_disconnected_new"]
            - x["playtime_forever_old"]
            - x["playtime_disconnected_old"]
        ),
        axis=1,
    )

    # 仅保留一列游戏名称，删除多余的列
    merge_game_info.drop("name_old", axis=1, inplace=True)
    merge_game_info.rename(columns={"name_new": "name"}, inplace=True)

    # 仅保留非零游戏时长的数据
    merge_game_info = merge_game_info[merge_game_info["playing_time"] > 0]
    merge_game_info = merge_game_info[["appid", "name", "playing_time"]]
    time = datetime.datetime.now(pytz.timezone("Asia/Shanghai"))

    # 调整字段顺序
    merge_game_info = merge_game_info[["appid", "name", "playing_time"]]
    # 记录数据日期，由于定时任务在凌晨执行，因此计算出来的游戏时长数据是昨天的
    merge_game_info["playtime_date"] = (
        datetime.datetime.now(pytz.timezone("Asia/Shanghai")) - datetime.timedelta(1)
    ).date()
    merge_game_info["creation_time"] = time

    # 保存数据
    os.makedirs("./data/playing_time_data", exist_ok=True)
    merge_game_info.to_csv(
        f"./data/playing_time_data/playing_time_{today_date}.csv", index=False
    )
def upload_to_notion():
    """
    将最近一天的游戏时长数据上传到Notion数据库
    """
    # 获取昨天的日期（数据日期）
    data_date = (datetime.now(pytz.timezone("Asia/Shanghai")) - timedelta(1)).strftime("%Y%m%d")
    csv_path = f"./data/playing_time_data/playing_time_{data_date}.csv"
    
    # 检查文件是否存在
    if not os.path.exists(csv_path):
        print(f"数据文件不存在: {csv_path}")
        return
    
    # 读取CSV数据
    df = pd.read_csv(csv_path)
    if df.empty:
        print("没有需要上传的数据")
        return
    
    # 获取环境变量
    notion_key = os.environ.get("NOTION_KEY")
    notion_database_id = os.environ.get("NOTION_DATABASE_ID")
    
    if not notion_key or not notion_database_id:
        raise ValueError("NOTION_KEY 或 NOTION_DATABASE_ID 未设置")
    
    # Notion API配置
    headers = {
        "Authorization": f"Bearer {notion_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 转换日期格式为Notion需要的格式
    df["playtime_date"] = pd.to_datetime(df["playtime_date"]).dt.strftime("%Y-%m-%d")
    
    # 上传每条记录
    for _, row in df.iterrows():
        # 构造页面属性
        page_properties = {
            "parent": {"database_id": notion_database_id},
            "properties": {
                "游戏名称": {
                    "title": [
                        {
                            "text": {
                                "content": row["name"]
                            }
                        }
                    ]
                },
                "游戏ID": {
                    "number": int(row["appid"])
                },
                "游戏时长(分钟)": {
                    "number": int(row["playing_time"])
                },
                "日期": {
                    "date": {
                        "start": row["playtime_date"]
                    }
                }
            }
        }
        
        # 发送创建页面请求
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json=page_properties
        )
        
        if response.status_code != 200:
            print(f"上传失败: {response.status_code} - {response.text}")
        else:
            print(f"成功上传: {row['name']} - {row['playing_time']}分钟")

# 更新主函数
if __name__ == "__main__":
    get_steam_data()
    merge_steam_data()
    get_playing_time()
    upload_to_notion()  # 添加Notion上传
    
    # ... 原有的图表生成代码保持不变 ...
