import json
import requests
import pandas as pd


displine_code = {
    "游泳": "SWM",
    "射箭": "ARC",
    "田径（马拉松）": "ATM",
    "羽毛球": "BDM",
    "篮球": "BKB",
    "拳击": "BOX",
    "竞速小轮车": "BMX",
    "马术": "EQU",
    "足球": "FBL",
    "艺术体操": "GRY",
    "排球": "VVO"
}

def get_schedule_simple(discipline, date="") -> dict:
    """
    获取单项赛程数据
    Args:
        discipline: 项目类型
        date: 日期，默认为空
    """
    url = "https://infoapi.baygames.cn/api/info/scheduleUnit/Discipline"
    
    params = {
        "Discipline": discipline,
        "Date": date
    }
    
    # 必要的请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Referer": "https://wrs.baygames.cn/",
        "Origin": "https://wrs.baygames.cn"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"请求失败，状态码: {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"错误: {e}")
        return {}
    
df_total = pd.DataFrame()

for name, code in displine_code.items():
    raw_data = get_schedule_simple(discipline=code, date="")
    if raw_data:
        # 将数据转换为DataFrame
        print("处理项目:", name)


        data = raw_data["Result"]["Disciplines"][0]["Units"]

        # 创建DataFrame
        df = pd.DataFrame(data)

        # 提取深圳的比赛数据
        df = df[df['CHI_VenueName'].str.contains('深圳', na=False)]

        # 展开Attach列中的嵌套数据
        def extract_attach_info(row):
            attach = row['Attach']
            if attach and 'Details' in attach and attach['Details'] and 'HeadToHead' in attach['Details']:
                head_to_head = attach['Details']['HeadToHead']
                if head_to_head and len(head_to_head) >= 2:
                    # 提取第一位参与者信息
                    row['Participant1_Code'] = head_to_head[0].get('ParticipantCode')
                    row['Participant1_Name'] = head_to_head[0].get('ParticipantName')
                    row['Participant1_Organisation'] = head_to_head[0].get('Organisation')
                    row['Participant1_Result'] = head_to_head[0].get('Result')
                    row['Participant1_Wlt'] = head_to_head[0].get('Wlt')
                    
                    # 提取第二位参与者信息
                    row['Participant2_Code'] = head_to_head[1].get('ParticipantCode')
                    row['Participant2_Name'] = head_to_head[1].get('ParticipantName')
                    row['Participant2_Organisation'] = head_to_head[1].get('Organisation')
                    row['Participant2_Result'] = head_to_head[1].get('Result')
                    row['Participant2_Wlt'] = head_to_head[1].get('Wlt')

                    # 整理对阵双方
                    if row['Participant1_Name'] is not None and row['Participant2_Name'] is not None:
                        row['Matchup'] = f"{row['Participant1_Name']} vs {row['Participant2_Name']}"
                    if row['Participant1_Result'] is not None and row['Participant2_Result'] is not None:
                        row['Matchup_Result'] = f"{row['Participant1_Result']} : {row['Participant2_Result']}"

                    # 删除临时列
                    del row['Participant1_Code']
                    del row['Participant2_Code']
                    del row['Participant1_Name']
                    del row['Participant2_Name']
                    del row['Participant1_Organisation']
                    del row['Participant2_Organisation']
                    del row['Participant1_Result']
                    del row['Participant2_Result']
                    del row['Participant1_Wlt']
                    del row['Participant2_Wlt']
            
            return row
        
        # 将StartDate和EndDate转换拆分为日期和时间
        df['StartTime'] = pd.to_datetime(df['StartDate']).dt.time
        df['EndTime'] = pd.to_datetime(df['EndDate']).dt.time

        df['Date'] = pd.to_datetime(df['StartDate']).dt.date


        # 应用函数展开嵌套数据
        df = df.apply(extract_attach_info, axis=1)

        # 删除原始的Attach列（可选）
        df = df.drop('Attach', axis=1)

        # 显示前几行数据
        print("数据表格的前5行:")
        print(df.head())

        # 显示数据框的基本信息
        print("\n数据框形状:", df.shape)
        print("\n列名:")
        print(df.columns.tolist())


        # 判断场馆名称和比赛地点是否重复
        if df['CHI_VenueName'].equals(df['CHI_LocationName']):
            print("\n场馆名称和比赛地点列内容相同，删除列")
            df = df.drop('CHI_LocationName', axis=1)

        # 将项目名称、比赛日期、开始时间、结束时间列移动到前面
        cols = df.columns.tolist()
        cols = [col for col in cols if col not in ['CHI_DisciplineName', 'ENG_DisciplineName', 'Date', 'StartTime', 'EndTime']]
        cols = ['CHI_DisciplineName', 'ENG_DisciplineName', 'Date', 'StartTime', 'EndTime'] + cols
        df = df[cols]

        # 分简体中文和繁体中文保存
        CHN_columns = [col for col in df.columns if col.startswith('CHI_') or col in [
            'Date', 'StartTime', 'EndTime', 'Medal',
            'Matchup', 'Matchup_Organisation', 'Matchup_Result'
            # 'Participant1_Name', 'Participant1_Organisation', 
            # 'Participant1_Result', 'Participant1_Wlt',
            # 'Participant2_Name', 'Participant2_Organisation', 
            # 'Participant2_Result', 'Participant2_Wlt'
        ]]

        ENG_columns = [col for col in df.columns if col.startswith('ENG_') or col in [
            'StartDate', 'EndDate', 'Medal',
            'Matchup', 'Matchup_Organisation', 'Matchup_Result'
            # 'Participant1_Name', 'Participant1_Organisation', 
            # 'Participant1_Result', 'Participant1_Wlt',
            # 'Participant2_Name', 'Participant2_Organisation', 
            # 'Participant2_Result', 'Participant2_Wlt'
        ]]

        df_chn = df[CHN_columns]
        df_eng = df[ENG_columns]

        # 将标题行中的列名变为中文
        column_rename_chn = {
            'CHI_LocationName': '比赛地点',
            'CHI_VenueName': '场馆名称',
            'CHI_EventName': '赛事名称',
            'CHI_ItemName': '比赛名称',
            'CHI_DisciplineName': '项目名称',
            'CHI_ScheduleUnitName': '赛程单元名称',
            'CHI_ScheduleStatusName': '当前赛程状态',
            'Date': '比赛日期',
            'StartTime': '开始时间',
            'EndTime': '结束时间',
            'Medal': '产生奖牌数',
            'Matchup': '对阵双方',
            'Matchup_Organisation': '对阵双方组织',
            'Matchup_Result': '对阵双方结果',
            # 'Participant1_Name': '参与者1姓名',
            # 'Participant1_Organisation': '参与者1组织',
            # 'Participant1_Result': '参与者1结果',
            # 'Participant1_Wlt': '参与者1胜负情况',
            # 'Participant2_Name': '参与者2姓名',
            # 'Participant2_Organisation': '参与者2组织',
            # 'Participant2_Result': '参与者2结果',
            # 'Participant2_Wlt': '参与者2胜负情况'
        }
        
        # 重命名列
        df_chn = df_chn.rename(columns=column_rename_chn)

        df_total = pd.concat([df_total, df_chn], ignore_index=True)

        # # 保存为Excel文件（可选）
        # df.to_excel(f'{name}_schedule.xlsx', index=False, engine='openpyxl')



        # 保存为CSV文件（可选）
        # df.to_csv(f'{name}_schedule.csv', index=False, encoding='utf-8-sig')
        # df_chn.to_csv(f'{name}_赛程.csv', index=False, encoding='utf-8-sig')
        # df_eng.to_csv(f'繁体_{name}_赛程.csv', index=False, encoding='utf-8-sig')

        print(f"\n数据已保存为Excel和CSV文件，共{len(df)}条记录")

# df_total.to_csv('1.所有项目_赛程.csv', index=False, encoding='utf-8-sig')
# df_total.to_excel('1.所有项目_赛程.xlsx', index=False, engine='openpyxl')
print(f"\n所有项目数据已保存为CSV文件，共{len(df_total)}条记录")



# 查找某一天的赛程
def find_schedule_by_date(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    根据日期查找赛程
    Args:
        df: 赛程数据DataFrame
        date: 日期字符串，格式为'YYYY-MM-DD'
    Returns:
        符合日期的赛程DataFrame
    """
    date = pd.to_datetime(date).date()
    filtered_df = df[df['比赛日期'] == date]
    return filtered_df


find_data = "2025-11-12"

df_found = find_schedule_by_date(df_total, find_data)


# 删去当前赛程状态、奖牌、对阵双方和对阵双方结果
columns_to_drop = ['当前赛程状态', '奖牌', '对阵双方', '对阵双方组织', '对阵双方结果']
df_found = df_found.drop(columns=columns_to_drop, errors='ignore')


print(f"\n查找日期 {find_data} 的赛程，共{len(df_found)}条记录:")
# print(df_found)

# df_found.to_excel(f'2.所有项目_赛程_{find_data}.xlsx', index=False, engine='openpyxl')




# df_total左侧加入序号列
df_total.insert(0, '序号', range(1, len(df_total) + 1))

# 导入飞书
from feishu import DataFrameToFeishu
feishu_app_id = "cli_a987003ea3f2101c"
feishu_app_secret = "4ZVHgixaB8njuIDgoTBJVeYr5WOh3nim"
feishu_sync = DataFrameToFeishu(feishu_app_id, feishu_app_secret)
# sheet_info = feishu_sync.sync_dataframe_to_new_sheet(df_total, title=f"深圳赛程数据汇总")
sheet_info = feishu_sync.sync_dataframe_to_existing_sheet(df_total, spreadsheet_token="ZN8Rsb3KyhzwGmtJY9jcZDZ4nHc")
print("飞书表格创建成功，表格信息：", sheet_info)








