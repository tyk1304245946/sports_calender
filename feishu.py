import pandas as pd
import requests
import json
from typing import List, Dict, Any
import time

import lark_oapi as lark
from lark_oapi.api.sheets.v3 import *

class DataFrameToFeishu:    
    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = self._get_access_token()
        self.client = lark.Client.builder().enable_set_token(True).log_level(lark.LogLevel.DEBUG).build()
    
    def _get_access_token(self) -> str:
        """获取飞书访问令牌"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        print("获取飞书访问令牌成功", response.json())
        return response.json()["tenant_access_token"]
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def create_new_sheet(self, title: str = "DataFrame数据"):
        """创建新的飞书表格"""
        # 构造请求对象
        request: CreateSpreadsheetRequest = CreateSpreadsheetRequest.builder() \
            .request_body(Spreadsheet.builder()
                .title(title)
                .folder_token("")
                .build()) \
            .build()

        # 发起请求
        option = lark.RequestOption.builder().user_access_token(self.access_token).build()
        response: CreateSpreadsheetResponse = self.client.sheets.v3.spreadsheet.create(request, option)

        # 处理失败返回
        if not response.success():
            lark.logger.error(
                f"client.sheets.v3.spreadsheet.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return

        # 处理业务结果
        data = lark.JSON.marshal(response.data, indent=4)
        lark.logger.info(data)
        data = json.loads(data)

        return data["spreadsheet"]

    def sync_dataframe_to_new_sheet(self, df: pd.DataFrame, title: str = None) -> Dict[str, Any]:
        """
        将DataFrame同步到新的飞书表格
        
        Args:
            df: 要同步的pandas DataFrame
            title: 表格标题，默认为"DataFrame数据_时间戳"
        
        Returns:
            创建的表格信息
        """
        if title is None:
            title = f"DataFrame数据_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 1. 创建新表格
        sheet_info = self.create_new_sheet(title)
        spreadsheet_token = sheet_info["spreadsheet_token"]
        
        # 2. 获取第一个sheet的ID
        sheets_info = self.get_sheets_info(spreadsheet_token)
        if not sheets_info:
            raise Exception("未找到可用的sheet")
        
        sheet_id = sheets_info[0]["sheet_id"]
        
        # 3. 准备数据
        values = self._prepare_dataframe_data(df)
        
        # 4. 写入数据
        self.write_data_to_sheet(spreadsheet_token, sheet_id, values)

        # 6. 规范日期显示
        self.normalize_date_format(spreadsheet_token, sheet_id, df)
        
        # 5. 可选：调整列宽
        self.auto_fit_columns(spreadsheet_token, sheet_id, df)

        # 7. 居中显示
        self.center_align_columns(spreadsheet_token, sheet_id, len(df.columns), len(df))

        
        print(f"数据同步成功！表格链接: https://example.feishu.cn/sheets/{spreadsheet_token}")
        return sheet_info
    
    def center_align_columns(self, spreadsheet_token: str, sheet_id: str, num_columns: int, num_rows: int):
        """将所有单元格内容居中对齐"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style"
        
        data = {
            "appendStyle":{
                "range": f"{sheet_id}!A1:{chr(64 + num_columns)}{num_rows + 1}",
                "style": {
                    "vAlign": 1,
                    "hAlign": 1
                }
            }
        }
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=data)
            response.raise_for_status()
            print(response.json())
        except Exception as e:
            print(f"设置居中对齐时出错: {e}")

    def normalize_date_format(self, spreadsheet_token: str, sheet_id: str, df: pd.DataFrame):
        """规范日期列的显示格式"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style"
        
        # 遍历DataFrame的列，将dt.time设置为yyyy-mm-dd, dt.time设置成hh:mm:ss
        # for col_idx, col in enumerate(df.columns):
            # if pd.api.types.is_datetime64_dtype(df[col]):
            # 设置为日期格式
        data = {
            "appendStyle":{
                "range": f"{sheet_id}!B2:B{df.shape[0]+1}",
                "style": {
                    "formatter": "@"
                }
            }
        }
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=data)
            response.raise_for_status()
            print(response.json())
        except Exception as e:
            print(f"设置列格式时出错: {e}")
    
    def sync_dataframe_to_existing_sheet(self, df: pd.DataFrame, spreadsheet_token: str, 
                                       sheet_id: str = None, range: str = "A1") -> Dict[str, Any]:
        """
        将DataFrame同步到已有的飞书表格
        
        Args:
            df: 要同步的pandas DataFrame
            spreadsheet_token: 表格token
            sheet_id: sheet ID，如果为None则使用第一个sheet
            range: 起始单元格位置
        """
        if sheet_id is None:
            sheets_info = self.get_sheets_info(spreadsheet_token)
            if not sheets_info:
                raise Exception("未找到可用的sheet")
            sheet_id = sheets_info[0]["sheet_id"]
        
        # 准备数据
        values = self._prepare_dataframe_data(df)
        
        # 写入数据
        result = self.write_data_to_sheet(spreadsheet_token, sheet_id, values)

        # 规范日期显示
        self.normalize_date_format(spreadsheet_token, sheet_id, df)
        
        # 调整列宽
        self.auto_fit_columns(spreadsheet_token, sheet_id, df)

        # 居中显示
        self.center_align_columns(spreadsheet_token, sheet_id, len(df.columns), len(df))
        
        print(f"数据同步到现有表格成功！")
        return result
    
    def _prepare_dataframe_data(self, df: pd.DataFrame) -> List[List[Any]]:
        """将DataFrame转换为飞书表格需要的二维数组格式"""
        # 处理列名
        columns = []
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                columns.append(f"{col}(日期时间)")
            elif pd.api.types.is_numeric_dtype(df[col]):
                columns.append(f"{col}(数值)")
            else:
                columns.append(str(col))
        
        # 处理数据
        values = [columns]  # 第一行为列名
        
        for _, row in df.iterrows():
            row_data = []
            for value in row:
                # 处理特殊数据类型
                if pd.isna(value):
                    row_data.append("")
                elif isinstance(value, pd.Timestamp):
                    row_data.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    row_data.append(str(value))
            values.append(row_data)
        
        return values
    
    def get_sheets_info(self, spreadsheet_token: str):
        """获取表格中的所有sheet信息"""
        # 构造请求对象
        request: QuerySpreadsheetSheetRequest = QuerySpreadsheetSheetRequest.builder() \
        .spreadsheet_token(spreadsheet_token) \
        .build()

        # 发起请求
        option = lark.RequestOption.builder().user_access_token(self.access_token).build()
        response: QuerySpreadsheetSheetResponse = self.client.sheets.v3.spreadsheet_sheet.query(request, option)

        # 处理失败返回
        if not response.success():
            lark.logger.error(
                f"client.sheets.v3.spreadsheet_sheet.query failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return

        # 处理业务结果
        data = lark.JSON.marshal(response.data, indent=4)
        lark.logger.info(data)
        data = json.loads(data)

        return data['sheets']
    
    def write_data_to_sheet(self, spreadsheet_token: str, sheet_id: str, 
                          values: List[List[Any]]) -> Dict[str, Any]:
        """将数据写入指定sheet"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        
        # 构建范围字符串
        # if "!" not in range:
        #     range = f"{sheet_id}!{range}"
        range = f"{sheet_id}"
        
        data = {
            "valueRange": {
                "range": range,
                "values": values
            }
        }
        
        response = requests.put(url, headers=self._get_headers(), json=data)
        response.raise_for_status()
        return response.json()["data"]
    
    def auto_fit_columns(self, spreadsheet_token: str, sheet_id: str, df: pd.DataFrame):
        """自动调整列宽"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
        
        
        # 为前num_columns列设置自动列宽
        for col in range(len(df.columns)):
            width = 105
            if df.columns[col] in ["比赛名称", "场馆名称"]:
                width = 250  # 设置较宽的列宽
            data = {
                "dimension": {
                    "sheetId": sheet_id,
                    "majorDimension": "COLUMNS",
                    "startIndex": col + 1,
                    "endIndex": col + 2
                },
                "dimensionProperties": {
                    "fixedSize": width
                }
            }
            
            try:
                requests.put(url, headers=self._get_headers(), json=data)
            except Exception as e:
                print(f"调整列宽时出错: {e}")
    
    def clear_sheet_data(self, spreadsheet_token: str, sheet_id: str = None):
        """清空sheet中的数据"""
        if sheet_id is None:
            sheets_info = self.get_sheets_info(spreadsheet_token)
            sheet_id = sheets_info[0]["sheet_id"]
        
        # 写入空数据来清空
        empty_data = [[""]]
        self.write_data_to_sheet(spreadsheet_token, sheet_id, empty_data, "A1:ZZ10000")

    def sync_large_dataframe_in_chunks(feishu_sync, df, spreadsheet_token, chunk_size=1000):
        """分块同步大数据集"""
        total_rows = len(df)
        
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk_df = df.iloc[start:end]
            
            # 如果是第一块，包含列名
            if start == 0:
                range_start = "A1"
            else:
                range_start = f"A{start + 2}"  # +2 因为第一行是列名
            
            feishu_sync.sync_dataframe_to_existing_sheet(
                df=chunk_df,
                spreadsheet_token=spreadsheet_token,
                range=range_start
            )
            
            print(f"已同步 {end}/{total_rows} 行数据")
            time.sleep(1)  # 避免API限制

# 使用分块同步
# sync_large_dataframe_in_chunks(feishu_sync, large_df, "your_spreadsheet_token")