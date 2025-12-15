import base64
import binascii
import json
import time

import requests
from urllib import parse
from concurrent.futures import ThreadPoolExecutor
import threading
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pksc1_v1_5
from Crypto.PublicKey import RSA
import logging
import argparse
from tqdm import tqdm

import requests
import os
from bs4 import BeautifulSoup
import time
import sqlite3
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlsplit, parse_qs
import re
import schedule
import time
from dotenv import load_dotenv
import os
from p123client import P123Client, check_response
from p189_export_share import create_189_rapid_transfer
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

# 加载.env文件中的环境变量
load_dotenv(dotenv_path="db/user.env",override=True)
load_dotenv(dotenv_path="sys.env",override=True)
# 安全地获取整数值，避免异常
def get_int_env(env_name, default_value=0):
    try:
        value = os.getenv(env_name, str(default_value))
        return int(value) if value else default_value
    except (ValueError, TypeError):
        TelegramNotifier(os.getenv("ENV_TG_BOT_TOKEN", ""), int(os.getenv("ENV_TG_ADMIN_USER_ID", "0"))).send_message(f"[警告] 环境变量 {env_name} 值不是有效的整数，使用默认值 {default_value}")
        logger.warning(f"环境变量 {env_name} 值不是有效的整数，使用默认值 {default_value}")
        return default_value
CHANNEL_URL = os.getenv("ENV_189_TG_CHANNEL", "")
ENV_189_TG_CHANNEL = os.getenv("ENV_189_TG_CHANNEL","")
ENV_189_CLIENT_ID = os.getenv("ENV_189_CLIENT_ID","")
ENV_189_CLIENT_SECRET = os.getenv("ENV_189_CLIENT_SECRET","")
ENV_189_UPLOAD_PID = os.getenv("ENV_189_UPLOAD_PID","")

TG_BOT_TOKEN = os.getenv("ENV_TG_BOT_TOKEN", "")
TG_ADMIN_USER_ID = get_int_env("ENV_TG_ADMIN_USER_ID", 0)

# 123 账号
CLIENT_ID = os.getenv("ENV_123_CLIENT_ID", "")
# 123 密码
CLIENT_SECRET = os.getenv("ENV_123_CLIENT_SECRET", "")

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 修改数据库文件路径到 db 目录下
DB_DIR = "db"
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)
DATABASE_FILE = os.path.join(DB_DIR, "TG_monitor-189.db")
CHECK_INTERVAL = get_int_env("ENV_CHECK_INTERVAL", 5)  # 检查间隔（分钟）
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
]
RETRY_TIMES = 3
TIMEOUT = 15

def rsaEncrpt(password, public_key):
    rsakey = RSA.importKey(public_key)
    cipher = Cipher_pksc1_v1_5.new(rsakey)
    return cipher.encrypt(password.encode()).hex()


def format_size(size):
    units = ['B', 'KB', 'MB', 'GB', 'TB']

    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"


config = {
    "clientId": '538135150693412',
    "model": 'KB2000',
    "version": '9.0.6',
    "pubKey": 'MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCZLyV4gHNDUGJMZoOcYauxmNEsKrc0TlLeBEVVIIQNzG4WqjimceOj5R9ETwDeeSN3yejAKLGHgx83lyy2wBjvnbfm/nLObyWwQD/09CmpZdxoFYCH6rdDjRpwZOZ2nXSZpgkZXoOBkfNXNxnN74aXtho2dqBynTw3NFTWyQl8BQIDAQAB',
}

def clean_filename(name):
    # 定义非法字符
    illegal_chars = '"\\/:*?|><'
    # 移除非法字符
    for char in illegal_chars:
        name = name.replace(char, '')
    # 限制长度为255个字符
    return name[:255]
class BatchSaveTask:
    def __init__(self, shareInfo, batchSize, targetFolderId, shareFolderId=None, maxWorkers=3):
        self.shareInfo = shareInfo
        self.batchSize = batchSize
        self.shareFolderId = shareFolderId
        self.targetFolderId = targetFolderId
        self.tqLock = threading.Lock()
        self.taskNum = 0
        self.walkDirNum = 0
        self.saveDirNum = 0
        self.savedFileNum = 0
        self.savedFileSize = 0
        self.failed = False
        self.threadPool = ThreadPoolExecutor(max_workers=maxWorkers)
        self.tq = tqdm(desc='正在保存')

    def __updateTq(self, num=1):
        data = {
            "剩余任务数": self.taskNum,
            "已保存文件数": self.savedFileNum,
            "已保存目录数:": self.saveDirNum,
            "已遍历目录数:": self.walkDirNum
            ##"已保存文件总大小": format_size(self.savedFileSize)
        }
        if num:
            self.tq.set_postfix(data, refresh=False)
            self.tq.update(num)
        else:
            self.tq.set_postfix(data)

    def __incTaskNum(self, num):
        self.tqLock.acquire()
        self.taskNum += num
        self.__updateTq(0)
        self.tqLock.release()

    def getTaskNum(self):
        self.tqLock.acquire()
        num = self.taskNum
        self.tqLock.release()
        return num

    def __incWalkDirNum(self, num=1):
        self.tqLock.acquire()
        self.walkDirNum += num
        self.__updateTq(num)
        self.tqLock.release()

    def __incSaveDirNum(self, num=1):
        self.tqLock.acquire()
        self.saveDirNum += num
        self.__updateTq(num)
        self.tqLock.release()

    def __incSavedFileInfo(self, fileInfos):
        fileNum = len(fileInfos)
        totalSize = 0
        for i in fileInfos:
            totalSize += i.get("size")
        self.tqLock.acquire()
        self.savedFileNum += fileNum
        self.savedFileSize += totalSize
        self.__updateTq(fileNum)
        self.tqLock.release()

    def run(self, checkInterval=1):
        with self.tq:
            self.__incTaskNum(1)
            self.threadPool.submit(self.__batchSave, self.targetFolderId, self.shareFolderId)
            while self.getTaskNum() > 0:
                time.sleep(checkInterval)
            self.threadPool.shutdown()
        return self.failed

    def __testAndSaveDir(self, folderInfo, targetFolderId):
        try:
            folderName = folderInfo["name"]
            shareFolderId = folderInfo["id"]
            # 清理文件夹名称中的非法字符
            clean_folder_name = clean_filename(folderName)
            code = self.shareInfo.saveShareFiles([{
                "fileId": shareFolderId,
                "fileName": clean_folder_name,
                "isFolder": 1}],
                targetFolderId)
            if code:
                if code == "ShareDumpFileOverload":
                    try:
                        nextFolderId = self.shareInfo.client.createFolder(parentFolderId=targetFolderId,
                                                                          name=folderName)
                        if nextFolderId:
                            self.__incTaskNum(1)
                            self.threadPool.submit(self.__batchSave, nextFolderId, shareFolderId)
                            return
                        else:
                            log.error(f"failed to create folder[{folderInfo}] at [{targetFolderId}]")
                            self.failed = True
                    except Exception as e1:
                        log.error(f"failed to create folder[{folderInfo}] at [{targetFolderId}]: {e1}")
                        self.failed = True
                else:
                    log.error(f"save dir response unknown code: {code}")
                    self.failed = True
            else:
                self.__incSaveDirNum()
        except Exception as e2:
            log.error(f"TestAndSaveDir occurred exception: {e2}")
            self.failed = True
        finally:
            self.__incTaskNum(-1)

    def __mustSave(self, saveFiles, targetFolderId):
        try:
            taskInfos = []
            for fileInfo in saveFiles:
                taskInfos.append(
                    {
                            "fileId": fileInfo.get("id"),
                            "fileName": clean_filename(fileInfo.get("name")),
                            "isFolder": 0
                        }
                )
            code = self.shareInfo.saveShareFiles(taskInfos, targetFolderId)
            if code:
                log.error(f"save only files response unexpected code [num={len(saveFiles)}][code: {code}]")
                self.failed = True
            else:
                self.__incSavedFileInfo(saveFiles)
                return
        except Exception as e1:
            log.error(f"mustSave occurred exception: {e1}")
            self.failed = True
        finally:
            self.__incTaskNum(-1)

    def __splitFileListAndSave(self, fileList: list, targetFolderId):
        for i in range(0, len(fileList), self.batchSize):
            if self.failed:
                return
            self.__incTaskNum(1)
            self.threadPool.submit(self.__mustSave, fileList[i: i + self.batchSize], targetFolderId)

    def __batchSave(self, targetFolderId, shareFolderId: None):
        try:
            rootFiles = self.shareInfo.getAllShareFiles(shareFolderId)
            self.__incWalkDirNum()
            
            # 确保rootFiles包含files和folders键
            files = rootFiles.get("files", [])
            folders = rootFiles.get("folders", [])
            
            self.__splitFileListAndSave(files, targetFolderId)

            for folderInfo in folders:
                if self.failed:
                    return
                self.__incTaskNum(1)
                self.threadPool.submit(self.__testAndSaveDir, folderInfo, targetFolderId)
            return
        except Exception as e1:
            log.error(f"batchSave occurred exception: {e1}")
        finally:
            self.__incTaskNum(-1)
        self.failed = True


class Cloud189ShareInfo:
    def __init__(self, shareDirFileId, shareId, shareMode, cloud189Client):
        self.shareDirFileId = shareDirFileId
        self.shareId = shareId
        self.session = cloud189Client.session
        self.client = cloud189Client
        self.shareMode = shareMode

    def getAllShareFiles(self, folder_id=None):
        if folder_id is None:
            folder_id = self.shareDirFileId
        fileList = []
        folders = []
        pageNumber = 1
        while True:
            result = self.session.get("https://cloud.189.cn/api/open/share/listShareDir.action", params={
                "pageNum": pageNumber,
                "pageSize": "10000",
                "fileId": folder_id,
                "shareDirFileId": self.shareDirFileId,
                "isFolder": "true",
                "shareId": self.shareId,
                "shareMode": self.shareMode,
                "iconOption": "5",
                "orderBy": "lastOpTime",
                "descending": "true",
                "accessCode": "",
            }).json()
            #print(result)
            if result['res_code'] != 0:
                raise Exception(result['res_message'])
            
            # 确保fileListAO存在且是字典
            if not isinstance(result.get("fileListAO"), dict):
                log.error(f"Invalid fileListAO format: {result}")
                break
            
            fileListAO = result["fileListAO"]
            
            # 确保fileList和folderList存在
            current_files = fileListAO.get("fileList", [])
            current_folders = fileListAO.get("folderList", [])
            
            # 只有当文件列表和文件夹列表都为空时才退出循环
            if fileListAO.get("fileListSize", 0) == 0 and len(current_folders) == 0:
                break
            
            fileList += current_files
            folders += current_folders
            #print(fileList)
            #print(folders)
            pageNumber += 1
        return {"files": fileList, "folders": folders}

    def saveShareFiles(self, tasksInfos, targetFolderId):
        """
        保存文件到指定路径
        :param tasksInfos: ["fileId":"32313191387622589","fileName":"高血脂食疗药膳.epub","isFolder":0]
        :param targetFolderId: 保存到当前账户的指定目录：12474193948415710
        :return: "ShareDumpFileOverload"、None
        """
        try:
            # 统一参数格式为str，与cloud189.py保持一致
            response = self.session.post("https://cloud.189.cn/api/open/batch/createBatchTask.action", data={
                "type": "SHARE_SAVE",
                "taskInfos": str(tasksInfos),
                "targetFolderId": targetFolderId,
                "shareId": self.shareId,
            })
            # 检查响应状态码
            if response.status_code != 200:
                log.error(f"保存文件请求失败，状态码: {response.status_code}")
                return f"HTTP_ERROR_{response.status_code}"
            
            # 检查响应内容是否为空
            if not response.content.strip():
                log.error("保存文件请求返回空响应")
                return "EMPTY_RESPONSE"
            
            # 尝试解析JSON
            result = response.json()
            if result["res_code"] != 0:
                log.error(f"保存文件失败: {result.get('res_message', '未知错误')}")
                return result.get('res_message', 'UNKNOWN_ERROR')
            
            return None
        except json.JSONDecodeError as e:
            log.error(f"JSON解析错误: {e}")
            return f"JSON_ERROR: {str(e)}"
        except Exception as e:
            log.error(f"保存文件时发生异常: {e}")
            return f"EXCEPTION: {str(e)}"
        taskId = result["taskId"]
        while True:
            result = self.session.post("https://cloud.189.cn/api/open/batch/checkBatchTask.action", data={
                "taskId": taskId,
                "type": "SHARE_SAVE"
            }).json()
            taskStatus = result["taskStatus"]
            errorCode = result.get("errorCode")
            if taskStatus != 3 or errorCode:
                break
            time.sleep(1)
        return errorCode

    def createBatchSaveTask(self, targetFolderId, batchSize, shareFolderId=None, maxWorkers=3):
        return BatchSaveTask(shareInfo=self, batchSize=batchSize, targetFolderId=targetFolderId,
                             shareFolderId=shareFolderId, maxWorkers=3)


class Cloud189:
    def __init__(self):
        self.session = requests.session()
        self.session.headers = {
            'User-Agent': f"Mozilla/5.0 (Linux; U; Android 11; {config['model']} Build/RP1A.201005.001) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 Ecloud/{config['version']} Android/30 clientId/{config['clientId']} clientModel/{config['model']} clientChannelId/qq proVersion/1.0.6",
            "Accept": "application/json;charset=UTF-8",
        }

    def getObjectFolderNodes(self, folderId=-11):
        """
        获取目录列表
        :param folderId: 目录ID
        :return: [{"isParent": "true", "name": "ePUBee图书", "pId": "-11", "id": "12474193948415710"}]
        """
        return self.session.post("https://cloud.189.cn/api/portal/getObjectFolderNodes.action", data={
            "id": folderId,
            "orderBy": 1,
            "order": "ASC"
        }).json()

    def empty_recycle_bin(self):
        """
        清空回收站
        :return: 是否成功
        """
        try:
            response = self.session.post("https://cloud.189.cn/api/open/batch/createBatchTask.action", data={
                "type": "EMPTY_RECYCLE",
                "taskInfos": "[]",
                "targetFolderId": "",
            })
            
            # 检查响应状态码
            if response.status_code != 200:
                log.error(f"清空回收站请求失败，状态码: {response.json()}")
                return False
            
            # 检查响应内容是否为空
            if not response.content.strip():
                log.error("清空回收站请求返回空响应")
                return False
            
            # 尝试解析JSON
            result = response.json()
            if result.get("res_code") != 0:
                log.error(f"清空回收站失败: {result.get('res_message', '未知错误')}")
                return False
            
            log.info("清空回收站成功")
            return True
        except json.JSONDecodeError as e:
            log.error(f"JSON解析错误: {e}")
            return False
        except Exception as e:
            log.error(f"清空回收站时发生异常: {e}")
            return False
            
    def list_files(self, folder_id: str = "-11") -> dict:
        """
        获取文件列表
        :param folder_id: 文件夹ID，默认为根目录(-11)
        :return: 文件列表数据
        """
        try:
            response = self.session.get(
                "https://cloud.189.cn/api/open/file/listFiles.action",
                params={
                    "folderId": folder_id,
                    "mediaType": 0,
                    "orderBy": "lastOpTime",
                    "descending": True,
                    "pageNum": 1,
                    "pageSize": 1000
                }
            )
            
            if response.status_code != 200:
                log.error(f"获取文件列表请求失败，状态码: {response.status_code}")
                return {}
            
            result = response.json()
            if result.get("res_code") != 0:
                log.error(f"获取文件列表失败: {result.get('res_message', '未知错误')}")
                return {}
            
            return result
        except json.JSONDecodeError as e:
            log.error(f"JSON解析错误: {e}")
            return {}
        except Exception as e:
            log.error(f"获取文件列表时发生异常: {e}")
            return {}
            
    def delete_files(self, file_ids: list) -> dict:
        """
        删除文件或文件夹
        :param file_ids: 要删除的文件信息列表，格式为：[{"fileId": "xxx", "fileName": "xxx", "isFolder": 1}]
        :return: 删除结果
        """
        try:
            # 创建删除任务
            task_params = {
                "type": "DELETE",
                "taskInfos": str(file_ids),
                "targetFolderId": "",
            }
            
            response = self.session.post(
                "https://cloud.189.cn/api/open/batch/createBatchTask.action",
                data=task_params
            )
            
            if response.status_code != 200:
                log.error(f"创建删除任务请求失败，状态码: {response.status_code}")
                return {
                    "success": False,
                    "message": f"创建删除任务请求失败，状态码: {response.status_code}"
                }
            
            result = response.json()
            if result.get("res_code") != 0:
                log.error(f"创建删除任务失败: {result.get('res_message', '未知错误')}")
                return {
                    "success": False,
                    "message": f"创建删除任务失败: {result.get('res_message', '未知错误')}"
                }
            
            task_id = result.get("taskId")
            if not task_id:
                log.error("创建删除任务成功，但未返回taskId")
                return {
                    "success": False,
                    "message": "创建删除任务成功，但未返回taskId"
                }
            
            # 检查任务状态
            start_time = time.time()
            max_timeout = 30  # 最大超时时间(秒)
            
            while True:
                # 检查是否超时
                if time.time() - start_time > max_timeout:
                    log.error(f"任务 {task_id} 执行超时")
                    return {
                        "success": False,
                        "message": "文件删除失败：任务执行超时",
                        "task_id": task_id
                    }
                
                # 检查任务状态
                status_response = self.session.post(
                    "https://cloud.189.cn/api/open/batch/checkBatchTask.action",
                    data={
                        "taskId": task_id,
                        "type": "DELETE"
                    }
                )
                
                if status_response.status_code != 200:
                    log.error(f"检查任务状态请求失败，状态码: {status_response.status_code}")
                    time.sleep(1)
                    continue
                
                status_result = status_response.json()
                if status_result.get("res_code") != 0:
                    log.error(f"检查任务状态失败: {status_result.get('res_message', '未知错误')}")
                    time.sleep(1)
                    continue
                
                task_status = status_result.get("taskStatus")
                
                if task_status == 4:  # 4表示任务成功完成
                    # 检查是否有失败的文件
                    failed_count = status_result.get("failedCount", 0)
                    
                    if failed_count > 0:
                        log.warning(f"任务 {task_id} 完成，但有 {failed_count} 个文件未成功删除")
                        return {
                            "success": True,
                            "partial": True,
                            "message": f"部分文件删除失败，共 {failed_count} 个",
                            "task_id": task_id,
                            "status": status_result,
                            "failed_count": failed_count
                        }
                    else:
                        log.info(f"任务 {task_id} 成功完成，所有文件已删除")
                        return {
                            "success": True,
                            "message": "文件删除成功",
                            "task_id": task_id,
                            "status": status_result
                        }
                elif task_status in [1, 3]:  # 1和3表示任务进行中
                    time.sleep(1)  # 暂停1秒后继续检查
                else:  # 其他状态视为失败
                    log.error(f"文件删除失败: 任务状态异常 {task_status}")
                    return {
                        "success": False,
                        "message": f"文件删除失败: 任务状态异常 {task_status}",
                        "task_id": task_id,
                        "status": status_result
                    }
        except json.JSONDecodeError as e:
            log.error(f"JSON解析错误: {e}")
            return {
                "success": False,
                "message": f"JSON解析错误: {str(e)}"
            }
        except Exception as e:
            log.error(f"删除文件失败：{str(e)}")
            return {
                "success": False,
                "message": f"删除文件失败：{str(e)}"
            }
            
    def delete_folder_contents(self, folder_id: str) -> dict:
        """
        删除指定ID文件夹下的所有子文件和子文件夹
        :param folder_id: 文件夹ID
        :return: 删除结果
        """
        try:
            log.info(f"开始删除文件夹 {folder_id} 下的所有内容")
            
            # 获取文件夹下的所有文件和子文件夹
            files_result = self.list_files(folder_id)
            
            if not files_result or not files_result.get("fileListAO"):
                log.warning(f"文件夹 {folder_id} 为空或获取文件列表失败")
                return {
                    "success": True,
                    "message": f"文件夹 {folder_id} 为空或获取文件列表失败"
                }
            
            file_list = files_result["fileListAO"].get("fileList", [])
            folder_list = files_result["fileListAO"].get("folderList", [])
            
            # 构造删除任务信息
            task_infos = []
            
            # 添加文件
            for file in file_list:
                # 检查id字段是否存在
                if "id" not in file:
                    log.warning(f"文件缺少id字段: {file}")
                    continue
                task_infos.append({
                    "fileId": file["id"],
                    "fileName": file.get("fileName", "未命名文件"),
                    "isFolder": 0
                })
            
            # 添加文件夹
            for folder in folder_list:
                # 检查id字段是否存在
                if "id" not in folder:
                    log.warning(f"文件夹缺少id字段: {folder}")
                    continue
                task_infos.append({
                    "fileId": folder["id"],
                    "fileName": folder.get("fileName", "未命名文件夹"),
                    "isFolder": 1
                })
            
            if not task_infos:
                log.info(f"文件夹 {folder_id} 下没有需要删除的内容")
                return {
                    "success": True,
                    "message": f"文件夹 {folder_id} 下没有需要删除的内容"
                }
            
            log.info(f"找到 {len(task_infos)} 个项目需要删除")
            
            # 执行删除
            delete_result = self.delete_files(task_infos)
            
            return delete_result
        except Exception as e:
            log.error(f"删除文件夹内容时发生异常: {e}")
            return {
                "success": False,
                "message": f"删除文件夹内容时发生异常: {str(e)}"
            }

    def getFolderIdByPath(self, path, folderId=-11):
        """
        通过路径获取目录ID
        :param path: 路径
        :param folderId: 起始目录ID
        :return: 目录ID
        """
        path = path.strip("/")
        if not path:
            return folderId
        for name in path.split("/"):
            found = False
            filesData = self.getObjectFolderNodes(folderId)
            for node in filesData:
                if node["name"] == name:
                    folderId = node["id"]
                    found = True
                    break
            if not found:
                return None
        return folderId

    def getShareInfo(self, link):
        # 尝试从查询参数中提取分享码，如果失败则尝试从路径中提取
        url = parse.urlparse(link)
        try:
            code = parse.parse_qs(url.query)["code"][0]
        except (KeyError, IndexError):
            # 从路径中提取分享码 (格式: /t/xxxx)
            path_parts = url.path.split('/')
            if len(path_parts) >= 3 and path_parts[1] == 't':
                code = path_parts[2]
            else:
                raise Exception("无法从分享链接中提取分享码")
        result = self.session.get("https://cloud.189.cn/api/open/share/getShareInfoByCodeV2.action", params={
            "shareCode": code
        }).json()
        #print(result)
        if result['res_code'] != 0:
            raise Exception(result['res_message'])
        return Cloud189ShareInfo(
            shareId=result["shareId"],
            shareDirFileId=result["fileId"],
            cloud189Client=self,
            shareMode=result["shareMode"]
        )

    def getEncrypt(self):
        result = self.session.post("https://open.e.189.cn/api/logbox/config/encryptConf.do", data={
            'appId': 'cloud'
        }).json()
        return result['data']['pubKey']

    def getRedirectURL(self):
        rsp = self.session.get('https://cloud.189.cn/api/portal/loginUrl.action?redirectURL=https://cloud.189.cn/web'
                               '/redirect.html?returnURL=/main.action')
        if rsp.status_code == 200:
            return parse.parse_qs(parse.urlparse(rsp.url).query)
        else:
            raise Exception(f"status code must be 200, but real is {rsp.status_code}")

    def getLoginFormData(self, username, password, encryptKey):
        query = self.getRedirectURL()
        resData = self.session.post('https://open.e.189.cn/api/logbox/oauth2/appConf.do', data={
            "version": '2.0',
            "appKey": 'cloud',
        }, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/76.0',
            "Referer": 'https://open.e.189.cn/',
            "lt": query["lt"][0],
            "REQID": query["reqId"][0],
        }).json()
        if resData.get('result') == '0':
            keyData = f"-----BEGIN PUBLIC KEY-----\n{encryptKey}\n-----END PUBLIC KEY-----"
            usernameEncrypt = rsaEncrpt(username, keyData)
            passwordEncrypt = rsaEncrpt(password, keyData)
            return {
                "returnUrl": resData['data']['returnUrl'],
                "paramId": resData['data']['paramId'],
                "lt": query['lt'][0],
                "REQID": query['reqId'][0],
                "userName": f"{{NRP}}{usernameEncrypt}",
                "password": f"{{NRP}}{passwordEncrypt}",
            }
        else:
            raise Exception(resData["msg"])

    def createFolderFromShareLink(self, link, parentFolderId):
        """
        从分享链接创建同名文件夹
        :param link: 分享链接
        :param parentFolderId: 父文件夹ID，默认为323141206736999024
        :return: 创建的文件夹ID
        """
        try:
            # 提取分享码
            # 尝试从查询参数中提取分享码，如果失败则尝试从路径中提取
            url = parse.urlparse(link)
            try:
                code = parse.parse_qs(url.query)["code"][0]
            except (KeyError, IndexError):
                # 从路径中提取分享码 (格式: /t/xxxx)
                path_parts = url.path.split('/')
                if len(path_parts) >= 3 and path_parts[1] == 't':
                    code = path_parts[2]
                else:
                    raise Exception("无法从分享链接中提取分享码")
            # 获取分享信息
            result = self.session.get("https://cloud.189.cn/api/open/share/getShareInfoByCodeV2.action", params={
                "shareCode": code
            }).json()
            if result['res_code'] != 0:
                log.error(f"获取分享信息失败: {result.get('res_message', '未知错误')}")
                return None
            # 提取文件名
            fileName = result['fileName']
            logger.info(f"原始文件名：{fileName}")
            if not fileName:
                log.error("分享信息中未找到fileName字段")
                return None
            
            # 清理文件名：移除非法字符并限制长度
            cleaned_fileName = clean_filename(fileName) + " " + time.strftime("[%m%d%H%M%S]")
            logger.info(f"清理后文件名：{cleaned_fileName}")
            
            # 创建文件夹
            folderId = self.createFolder(cleaned_fileName, parentFolderId)
            return folderId
        except Exception as e:
            log.error(f"从分享链接创建文件夹时发生异常: {e}")
            return None

    def login(self, username, password):
        notifier = TelegramNotifier(TG_BOT_TOKEN, TG_ADMIN_USER_ID)
        encryptKey = self.getEncrypt()
        formData = self.getLoginFormData(username, password, encryptKey)
        data = {
            "appKey": 'cloud',
            "version": '2.0',
            "accountType": '01',
            "mailSuffix": '@189.cn',
            "validateCode": '',
            "returnUrl": formData['returnUrl'],
            "paramId": formData['paramId'],
            "captchaToken": '',
            "dynamicCheck": 'FALSE',
            "clientType": '1',
            "cb_SaveName": '0',
            "isOauth2": "false",
            "userName": formData['userName'],
            "password": formData['password'],
        }
        result = self.session.post('https://open.e.189.cn/api/logbox/oauth2/loginSubmit.do', data=data, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/76.0',
            'Referer': 'https://open.e.189.cn/',
            'lt': formData['lt'],
            'REQID': formData['REQID'],
        }).json()
        if result['result'] == 0:
            self.session.get(result['toUrl'], headers={
                "Referer": 'https://m.cloud.189.cn/zhuanti/2016/sign/index.jsp?albumBackupOpened=1',
                'Accept-Encoding': 'gzip, deflate',
                "Host": 'cloud.189.cn',
            })
        else:
            notifier.send_message(f"天翼云盘登录失败，失败原因：{result['msg']}")

    def createFolder(self, name, parentFolderId=-11):
        """
        创建目录返回文件ID
        :param parentFolderId: 父目录ID
        :param name: 要创建的文件ID
        :return:
        """
        result = self.session.post("https://cloud.189.cn/api/open/file/createFolder.action", data={
            "parentFolderId": parentFolderId,
            "folderName": name,
        }).json()
        if result["res_code"] != 0:
            raise Exception(result["res_message"])
        return result["id"]

    def mkdirAll(self, path, parentFolderId=-11):
        """
        创建所有路径
        :param path: 需要创建的路径
        :param parentFolderId: 父目录ID
        :return: 创建完成的目录ID
        """
        path = path.strip("/")
        if path:
            for name in path.split("/"):
                parentFolderId = self.createFolder(name=name, parentFolderId=parentFolderId)
        return parentFolderId


def getArgs():
    parser = argparse.ArgumentParser(description="天翼云盘保存分享文件(无单次转存上限)")
    parser.add_argument('-l', help='分享链接(形如 https://cloud.189.cn/web/share?code=XXXXXXXXX)', required=True)
    parser.add_argument('-u', help='云盘用户名', required=True)
    parser.add_argument('-p', help='云盘用户密码', required=True)
    parser.add_argument('-d', help='保存到的云盘的路径(不存在会自动创建, 形如: /A/B)', required=True)
    parser.add_argument('-t', help='转存线程数', default=5)
    return parser.parse_args()


def save_189_link(client : Cloud189, link, parentFolderId):
    notifier = TelegramNotifier(TG_BOT_TOKEN, TG_ADMIN_USER_ID)
    log.info("正在获取文件分享信息...")
    info = None
    try:
        info = client.getShareInfo(link)
    except Exception as e:
        log.error(f"获取分享信息出现错误: 链接错误或链接正在审核中 {e}")
        notifier.send_message(f"获取分享信息出现错误: 链接错误或链接正在审核中 {e}")
        return 0
    log.info("正在检查并创建目录...")
    saveDir = None
    try:
        saveDir = client.createFolderFromShareLink(link,parentFolderId)
    except Exception as e:
        log.error(f"检查并创建目录出现错误: {e}")
        notifier.send_message(f"检查并创建目录出现错误: {e}")
        return 0
    if not saveDir:
        log.error("无法获取保存目录信息，请检查天翼账号登录情况")
        notifier.send_message(f"无法获取保存目录信息，请检查天翼账号登录情况")
        return 0
    else:
        log.info("开始转储分享文件,耗时较长请耐心等待...")
        ret = info.createBatchSaveTask(saveDir, 500, maxWorkers=5).run()
        if not ret:
            log.info("所有分享文件已保存.")
            return 1
        else:
            log.error("保存分享文件出现出现错误：重复转存或其他异常")
            notifier.send_message(f"保存分享文件出现出现错误：重复转存或其他异常")
            return 0

def init_database():
    """初始化数据库（增加转存状态字段）"""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.execute('''CREATE TABLE IF NOT EXISTS messages
                    (
                        msg_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        id              TEXT,
                        date            TEXT,
                        message_url     TEXT,
                        target_url      TEXT,
                        transfer_status TEXT,
                        transfer_time   TEXT,
                        transfer_result TEXT,
                        msg_type        TEXT
                    )''')
    conn.execute("DELETE FROM messages WHERE datetime(transfer_time) < datetime(?,'-7 days')",(datetime.now().isoformat(),))
    conn.execute('''CREATE TABLE IF NOT EXISTS retry_messages
                    (
                        msg_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        id              TEXT,
                        date            TEXT,
                        message_url     TEXT,
                        target_url      TEXT,
                        transfer_time   TEXT,
                        transfer_result TEXT,
                        retry_num       INT,
                        retry_time      TEXT,
                        json_data       TEXT,
                        transfer_id     TEXT,
                        status          TEXT
                    )''')
    conn.execute("DELETE FROM retry_messages WHERE datetime(transfer_time) < datetime(?,'-7 days')",(datetime.now().isoformat(),))
    conn.commit()
    conn.close()

class TelegramNotifier:
    def __init__(self, bot_token, user_id):
        self.bot_token = bot_token
        self.user_id = user_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/" if self.bot_token else None

    def send_message(self, message):
        """向指定用户发送消息，若bot_token未设置则跳过发送，失败自动重试"""
        # 局部变量定义重试参数
        max_retries = 30  # 重试次数
        retry_delay = 60  # 重试间隔

        # 检查bot_token是否存在
        if not self.bot_token:
            logger.error("未设置bot_token，跳过发送消息")
            return False
        if not message:
            logger.error("警告：消息内容不能为空")
            return False
        success_count = 0
        fail_count = 0
        params = {
            "chat_id": self.user_id,
            "text": message
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}sendMessage",
                    params=params,
                    timeout=15  # 使用全局超时配置
                )
                response.raise_for_status()
                result = response.json()
                if result.get("ok", False):
                    logger.info(f"消息 '{message.replace('\n', '').replace('\r', '')[:20]}...' ，已成功发送给用户 {TG_ADMIN_USER_ID}（第{attempt+1}/{max_retries}次尝试）")
                    success_count += 1
                    break  # 成功则终止重试
                else:
                    error_msg = result.get('description', '未知错误')
                    logger.error(f"发送回复失败，{retry_delay}秒后重发，消息：{message}，错误：{error_msg}")
                    fail_count += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"发送回复失败，{retry_delay}秒后重发，消息：{message}，错误：{str(e)}")
                fail_count += 1

            # 非最后一次尝试则等待重试
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

        #logger.info(f"消息发送完成 - 成功: {success_count}, 失败: {fail_count}")
        return success_count > 0  # 保持原有返回值逻辑

def is_message_processed(message_url,msg_type = "1"):
    """检查消息是否已处理（无论转存是否成功）"""
    conn = sqlite3.connect(DATABASE_FILE)
    result = conn.execute("SELECT 1 FROM messages WHERE message_url = ? and msg_type = ?",
                          (message_url,msg_type)).fetchone()
    conn.close()
    return result is not None

def save_message(message_id, date, message_url, target_url,
                 status="待转存", result="", transfer_time=None, msg_type = "1"):
    """保存消息到数据库，包含转存状态"""
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        conn.execute("INSERT INTO messages (id, date, message_url, target_url, transfer_status, transfer_time, transfer_result, msg_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (message_id, date, message_url, target_url,
                      status, transfer_time or datetime.now().isoformat(), result,msg_type))
        conn.commit()
        logger.info(f"已记录: {message_id} | {target_url} | 状态: {status}")
    except sqlite3.IntegrityError:
        # 更新已有记录的状态
        conn.execute("UPDATE messages SET transfer_status=?, transfer_result=?, transfer_time=? WHERE id=?",
                     (status, result, transfer_time or datetime.now().isoformat(), message_id))
        conn.commit()
    finally:
        conn.close()

def save_retry_message(message_id, date, message_url, target_url, result="", transfer_time=None, json_data = None, transfer_id = None, retry_num = 0):
    """保存重试消息到数据库"""
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        conn.execute("INSERT INTO retry_messages (id, date, message_url, target_url, transfer_time, transfer_result, retry_num, retry_time, json_data, transfer_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (message_id, date, message_url, target_url,
                        transfer_time or datetime.now().isoformat(), result, retry_num, transfer_time or datetime.now().isoformat(), json_data, transfer_id, '0'))
        conn.commit()
        logger.info(f"重试已记录: {message_id} | {target_url} | 重试次数: {retry_num}")
    finally:
        conn.close()

def update_retry_message(message_id, target_url, json_data = None, retry_num = 0, status = '0'):
    """修改重试消息到数据库"""
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        # 更新已有记录的状态
        conn.execute("UPDATE retry_messages SET retry_time=?, retry_num=?, json_data=?, status=? WHERE msg_id=?",
                     (datetime.now().isoformat(), retry_num, json_data, status, message_id))
        conn.commit()
        logger.info(f"重试已记录: {message_id} | {target_url} | 重试次数: {retry_num}")
    finally:
        conn.close()

def get_latest_messages(msg_type = "1"):
    """获取最新消息（从最后一条开始检查）"""
    try:
        # 获取多个频道链接
        channel_urls = os.getenv("ENV_189_TG_CHANNEL", "").split('|')
        if not channel_urls or channel_urls == ['']:
            logger.warning("未配置ENV_189_TG_CHANNEL环境变量")
            return []
            
        all_new_messages = []
        
        for channel_idx, channel_url in enumerate(channel_urls):
            channel_url = channel_url.strip()
            if not channel_url:
                continue

            if channel_url.startswith('https://t.me/') and '/s/' not in channel_url:
                # 提取频道名称部分
                channel_name = channel_url.split('https://t.me/')[-1]
                # 重构URL，添加/s/
                channel_url = f'https://t.me/s/{channel_name}'

            logger.info(f"===== 处理第{channel_idx + 1}个频道: {channel_url} =====")
            
            session = requests.Session()
            retry = Retry(total=RETRY_TIMES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
            session.mount("https://", HTTPAdapter(max_retries=retry))
            headers = {"User-Agent": USER_AGENTS[int(time.time()) % len(USER_AGENTS)]}
            response = session.get(channel_url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            message_divs = soup.find_all('div', class_='tgme_widget_message')
            total = len(message_divs)
            logger.info(f"共解析到{total}条消息（最新的在最后）")

            new_messages = []

            for i in range(total):
                msg_index = total - 1 - i  # 从最后一条（最新）开始
                msg = message_divs[msg_index]
                data_post = msg.get('data-post', '')
                message_id = data_post.split('/')[-1] if data_post else f"未知ID_{msg_index}"
                logger.info(f"检查第{i + 1}新消息（倒数第{i + 1}条，ID: {message_id}）")

                time_elem = msg.find('time')
                date_str = time_elem.get('datetime') if time_elem else datetime.now().isoformat()
                link_elem = msg.find('a', class_='tgme_widget_message_date')
                message_url = f"{link_elem.get('href').lstrip('/')}" if link_elem else ''
                text_elem = msg.find('div', class_='tgme_widget_message_text')

                if text_elem:
                    # 提取消息文本内容（清理空格和换行）
                    message_text = text_elem.get_text(strip=True).replace('\n', ' ')
                    # print(str(text_elem))
                    target_urls = extract_target_url(f"{msg}")
                    if target_urls:
                        for url in target_urls:
                            if not is_message_processed(message_url,msg_type):
                                new_messages.append((message_id, date_str, message_url, url, message_text))
                                logger.info(message_url)
                            else:
                                logger.info(f"第{i + 1}新消息已处理，跳过")
                            logger.info(f"tg消息链接：{message_url}")
                            logger.info(f"天翼网盘链接：{url}")
                    else:
                        logger.info("未发现目标天翼网盘链接")
            
            all_new_messages.extend(new_messages)
        
        # 按时间正序排列所有消息
        all_new_messages.sort(key=lambda x: x[1])
        logger.info(f"===== 所有频道处理完成，共发现{len(all_new_messages)}条新的天翼网盘分享链接 =====")
        return all_new_messages

    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {str(e)[:100]}")
        return []


def get_retry_messages(max_retries=5, time_interval_minutes=60):
    """获取需要重试的消息

    Args:
        max_retries: 最大重试次数，默认5次
        time_interval_minutes: 时间间隔（分钟），默认60分钟

    Returns:
        list: 重试消息字典列表
    """
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            conn.row_factory = sqlite3.Row

            # 使用参数化查询，提高可读性和安全性
            query = """
                    SELECT msg_id, \
                           date, \
                           message_url, \
                           target_url, \
                           retry_num, \
                           retry_time, \
                           json_data, \
                           transfer_id
                    FROM retry_messages
                    WHERE datetime(retry_time) < datetime(?, ?)
                      AND retry_num < ?
                      AND status = '0'
                    ORDER BY retry_time ASC \
                    """

            now_time = datetime.now().isoformat()
            time_modifier = f"-{time_interval_minutes} minutes"

            rows = conn.execute(query, (now_time, time_modifier, max_retries)).fetchall()

            # 使用列表推导式简化代码
            retry_messages = [dict(row) for row in rows]

            if retry_messages:
                logger.info(f"获取到 {len(retry_messages)} 条需要重试的消息")
            else:
                logger.debug("未找到需要重试的消息")

            return retry_messages

    except sqlite3.Error as e:
        logger.error(f"数据库错误: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"获取重试消息错误: {str(e)}")
        return []


def extract_target_url(text):
    """
    从文本中精准提取天翼云盘链接，处理重复和访问码

    参数:
        text: 包含天翼云盘链接的文本

    返回:
        list: 标准化后的链接列表，已去重并优先保留带访问码的版本
    """
    # 增强版正则表达式，匹配两种URL格式和访问码
    pattern = r'''
        (https?://cloud\.189\.cn/  # 协议和域名
        (?:                         # 路径格式
            t/\w+|                  # /t/xxxx
            web/share\?code=\w+     # /web/share?code=xxxx
        ))
        (?:\s*（访问码：\s*([0-9a-zA-Z]{4})）)?  # 访问码部分
    '''

    matches = re.findall(pattern, text, re.IGNORECASE | re.VERBOSE)

    # 存储结果：{标准化URL: (访问码, 原始URL)}
    url_dict = defaultdict(list)

    for match in matches:
        raw_url = match[0].strip()
        access_code = match[1] if len(match) > 1 and match[1] else None

        # 标准化URL处理
        parsed = urlparse(raw_url)
        if not parsed.scheme:
            raw_url = 'https://' + raw_url
            parsed = urlparse(raw_url)

        # 重建标准化URL（统一协议和域名）
        normalized_url = urlunparse((
            'https',
            'cloud.189.cn',
            parsed.path,
            parsed.params,
            parsed.query,
            ''
        ))

        # 存储到字典
        url_dict[normalized_url].append((access_code, raw_url))

    # 处理每个URL，优先选择带访问码的版本
    results = []
    for url, versions in url_dict.items():
        # 找出所有带访问码的版本
        coded_versions = [v for v in versions if v[0]]

        if coded_versions:
            # 优先选择带访问码的版本（取第一个）
            selected = coded_versions[0]
        else:
            # 没有带访问码的版本，取第一个原始版本
            selected = versions[0]

        # 构建结果字符串
        result = url
        if selected[0]:
            result += f"（访问码：{selected[0]}）"

        results.append({
            'url': url,
            'access_code': selected[0] if selected[0] else None,
            'full_url': result
        })

    return results

def tg_189monitor(client189, client123, optimized_etag_to_hex, robust_normalize_md5):
    init_database()
    link_save_method = get_int_env("ENV_189_LINK_UPLOAD_METHOD", 2)
    
    notifier = TelegramNotifier(TG_BOT_TOKEN, TG_ADMIN_USER_ID)
    logger.info(f"===== 开始检查 天翼网盘监控（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）=====")

    logger.info("开始处理天翼网盘监控转存")
    if link_save_method == 3 or link_save_method == 1:
        new_messages = get_latest_messages()
        #schedule.run_pending()
        if new_messages:
            for msg in new_messages:
                message_id, date_str, message_url, target_url, message_text = msg
                logger.info(f"[天翼网盘转存]处理新消息: {message_id} | {target_url}")

                # 检查target_url是否为字典，如果是则提取full_url或url字段，否则直接使用target_url作为full_url
                if isinstance(target_url, dict):
                    full_url = target_url.get('full_url', '')
                    if not full_url:
                        full_url = target_url.get('url', '')
                else:
                    full_url = target_url

                # 转存到189
                result = save_189_link(client189, full_url, ENV_189_UPLOAD_PID)
                if result:
                    status = "转存成功"
                    result_msg = f"[天翼网盘转存]\n✅天翼云盘转存成功\n消息内容: {message_url}\n链接: {full_url}"
                else:
                    status = "转存失败"
                    result_msg = f"[天翼网盘转存]\n❌天翼云盘转存失败\n消息内容: {message_url}\n链接: {full_url}"

                notifier.send_message(result_msg)

                # 保存结果到数据库
                save_message(message_id, date_str, message_url, full_url, status, result_msg)
        else:
            logger.info("[天翼网盘转存]未发现新的天翼网盘分享链接")

    logger.info("处理天翼网盘监控转存结束")
    logger.info("开始处理天翼网盘监控转存123云盘")
    if link_save_method == 2 or link_save_method == 1:
        new_messages = get_latest_messages("2")
        if new_messages:
            for msg in new_messages:
                message_id, date_str, message_url, target_url, message_text = msg
                logger.info(f"[天翼网盘转存123云盘]处理新消息: {message_id} | {target_url}")

                # 检查target_url是否为字典，如果是则提取full_url或url字段，否则直接使用target_url作为full_url
                if isinstance(target_url, dict):
                    url = target_url.get('url', '')
                    full_url = target_url.get('full_url', '')
                    access_code = target_url.get('access_code', '')
                    if not full_url:
                        full_url = target_url.get('url', '')
                else:
                    full_url = target_url

                # 获取排除关键词环境变量（多个关键词用|分隔）
                # 当排除关键词为空时，全都不排除
                exclude_filter = os.environ.get('ENV_189_TO_123_EXCLUDE_FILTER', '')
                exclude_pattern = re.compile(exclude_filter) if exclude_filter else None

                FILTER = os.getenv("ENV_189_TO_123_FILTER", "")
                filter_pattern = re.compile(FILTER, re.IGNORECASE)

                # 检查是否匹配过滤条件且不包含排除关键词
                is_match = filter_pattern.search(full_url) or filter_pattern.search(message_text)
                is_excluded = exclude_pattern and (
                            exclude_pattern.search(full_url) or exclude_pattern.search(message_text))

                if not is_match:
                    status = "未转存"
                    result_msg = f"[天翼网盘转存123云盘]消息（{message_url}）未匹配过滤条件（{FILTER}），跳过转存"
                    logger.info(result_msg)
                    time.sleep(1)
                elif is_excluded:
                    status = "未转存"
                    result_msg = f"[天翼网盘转存123云盘]消息（{message_url}）包含排除关键词（{exclude_filter}），跳过转存"
                    logger.info(result_msg)
                    time.sleep(1)
                else:
                    logger.info(f"[天翼网盘转存123云盘]消息匹配过滤条件（{FILTER}），开始转存...")

                    # 二次过滤关键词配置（当某条消息触发转存后，如进一步满足下面的要求，则转移到特定的文件夹）
                    # 格式为：DV:1,DOLBY VISION:2,SSTA:3 即满足DV关键词转移到ID为1的文件夹，满足SSTA关键词转移到ID为3的文件夹
                    # 如果ENV_SECOND_FILTER为空，则全部转移至ENV_123_UPLOAD_PID
                    UPLOAD_TARGET_PID = os.getenv("ENV_189_TO_123_UPLOAD_PID", "0")
                    ENV_SECOND_FILTER = os.getenv("ENV_189_TO_123_SECOND_FILTER", "")
                    transfer_id = UPLOAD_TARGET_PID

                    # 根据关键词筛选并设置transfer_id
                    # ENV_SECOND_FILTER.strip() 用于去除字符串前后的空白字符（空格、制表符、换行符等）
                    # 这样可以确保即使环境变量值前后有空格也能正确处理，避免因空白字符导致的逻辑错误
                    # 如果去除空白后字符串不为空，则执行二次过滤逻辑
                    if ENV_SECOND_FILTER.strip():
                        try:
                            # 解析二次过滤规则，格式为：关键词:文件夹ID,关键词:文件夹ID,...
                            filter_rules = ENV_SECOND_FILTER.split(',')
                            for rule in filter_rules:
                                if ':' in rule:
                                    # 分割关键词和文件夹ID，但保留关键词中的空格（如"DOLBY VISION"中的空格会被保留）
                                    keyword, folder_id = rule.split(':', 1)
                                    # keyword.strip() 用于确保关键词不为空字符串
                                    # 注意：关键词内部的空格（如"DOLBY VISION"中的空格）不会被去除，会作为关键词的一部分进行匹配
                                    if (keyword.strip() and
                                            (keyword in message_text or
                                             (full_url and keyword in full_url))):
                                        transfer_id = int(folder_id.strip())
                                        logger.info(f"[天翼网盘转存123云盘]\n消息匹配二次过滤关键词 '{keyword}'，将转存到文件夹ID: {folder_id}")
                                        notifier.send_message(f"[天翼网盘转存123云盘]\n消息匹配二次过滤关键词 '{keyword}'，将转存到文件夹ID: {folder_id}")
                                        break
                        except Exception as e:
                            logger.error(f"[天翼网盘转存123云盘]解析二次过滤规则失败: {e}")
                            notifier.send_message(f"[天翼网盘转存123云盘]解析二次过滤规则失败: {e}")

                    json_data = create_189_rapid_transfer(url, access_code)
                    if json_data:
                        res = save_json_file_189(notifier, json_data, client123, optimized_etag_to_hex, robust_normalize_md5, transfer_id, message_url, full_url)
                        status = "转存成功"
                        result_msg = f"[天翼网盘转存123云盘]\n✅天翼云盘转存123云盘成功\n消息内容: {message_url}\n链接: {full_url}"
                        if not res is None:
                            # 保存结果到数据库
                            save_retry_message(message_id, date_str, message_url, full_url, result_msg, None, res, transfer_id)
                    else:
                        status = "转存失败"
                        result_msg = f"[天翼网盘转存123云盘]\n❌天翼云盘转存123云盘失败\n消息内容: {message_url}\n链接: {full_url}"

                notifier.send_message(result_msg)

                # 保存结果到数据库
                save_message(message_id, date_str, message_url, full_url, status, result_msg, None, "2")
            else:
                logger.info("[天翼网盘转存123云盘]未发现新的天翼网盘分享链接")

    logger.info("处理天翼网盘监控转存123云盘结束")
    logger.info("开始处理天翼网盘监控转存123云盘重试")
    # 重试转存代码
    RETRY_NUM = os.getenv("ENV_189_TO_123_RETRY_NUM", 5)
    RETRY_TIME = os.getenv("ENV_189_TO_123_RETRY_TIME", 60)
    new_messages = get_retry_messages(RETRY_NUM, RETRY_TIME)
    if new_messages:
        for msg in new_messages:
            logger.info(f"[189转存123重试]开始重试189转存123链接=>{msg}")
            res = save_json_file_189(notifier, json.loads(msg['json_data']), client123, optimized_etag_to_hex, robust_normalize_md5, msg['transfer_id'],
                               msg['message_url'], msg['target_url'],'189转存123重试', msg['retry_num'] + 1)
            result_msg = f"[189转存123重试]\n✅天翼云盘转存123云盘成功\n消息内容: {msg['message_url']}\n链接: {msg['target_url']}"
            notifier.send_message(result_msg)
            if res is None:
                # 保存结果到数据库
                update_retry_message(msg['msg_id'], msg['target_url'], msg['json_data'], msg['retry_num'] + 1, '1')
            else:
                # 保存结果到数据库
                update_retry_message(msg['msg_id'], msg['target_url'], res, msg['retry_num'] + 1)
    logger.info("[189转存123重试]处理天翼网盘监控转存123云盘重试结束")


from collections import defaultdict
def save_json_file_189(notifier,json_data, client123, optimized_etag_to_hex, robust_normalize_md5, target_dir_id, message_url, target_url, title = '自动转存', retry_num = 0):
    logger.info("进入123转存189")
    try:
        # 开始计时
        start_time = time.time()
        # 提取commonPath、files、totalFilesCount和totalSize
        common_path = json_data.get('commonPath', '').strip()
        common_path = re.sub(r'[\\/:*?|><"]', '', common_path)
        if common_path.endswith('/'):
            common_path = common_path[:-1]
        files = json_data.get('files', [])
        uses_v2_etag = json_data.get('usesBase62EtagsInExport', False)
        total_files_count = json_data.get('totalFilesCount', len(files))
        total_size_json = json_data.get('totalSize', 0)

        if retry_num > 0:
            title = f'[{title}](第{str(retry_num)}次重试)\n'
        else:
            title = f'[{title}]\n'

        if not files:
            notifier.send_message(f"{title}189分享中没有找到文件信息。")
            return None

        # 使用线程池发送回复
        notifier.send_message(f"{title}消息（{message_url}）\n开始123转存189链接（[{target_url}]）中的{len(files)}个文件...")
        start_time = time.time()

        # 转存文件
        results = []
        total_files = len(files)
        message_batch = []  # 用于存储每批消息(包括成功和失败)
        batch_size = 0  # 批次大小计数器
        total_size = 0  # 累计成功转存文件体积(字节)
        skip_count = 0  # 跳过的重复文件数量
        last_etag = None  # 上一个成功转存文件的etag

        # 创建文件夹缓存
        folder_cache = {}
        target_dir_name = common_path if common_path else 'JSON转存'
        # 使用UPLOAD_TARGET_PID作为根目录
        # target_dir_id = get_int_env("ENV_123_189_UPLOAD_PID", 0)

        # files失败的项
        error_files = []

        for i, file_info in enumerate(files):
            file_path = file_info.get('path', '')

            # 构建完整文件路径
            if common_path:
                file_path = f"{common_path}/{file_path}"
            etag = file_info.get('etag', '')
            size = int(file_info.get('size', 0))

            if not all([file_path, etag, size]):
                results.append({
                    "success": False,
                    "file_name": file_path or "未知文件",
                    "error": "文件信息不完整"
                })
                continue

            try:
                # 处理文件路径
                path_parts = file_path.split('/')
                file_name = path_parts.pop()
                parent_id = target_dir_id

                # 创建目录结构
                current_path = ""
                for part in path_parts:
                    if not part:
                        continue

                    current_path = f"{current_path}/{part}" if current_path else part
                    cache_key = f"{parent_id}/{current_path}"

                    # 检查缓存
                    if cache_key in folder_cache:
                        parent_id = folder_cache[cache_key]
                        continue

                    # 创建新文件夹（带重试）
                    retry_count = 3
                    folder = None
                    while retry_count > 0:
                        try:
                            folder = client123.fs_mkdir(part, parent_id=parent_id, duplicate=1)
                            time.sleep(0.2)
                            check_response(folder)
                            break
                        except Exception as e:
                            retry_count -= 1
                            logger.warning(f"{title}创建文件夹 {part} 失败 (剩余重试: {retry_count}): {str(e)}")
                            time.sleep(31)

                    if not folder:
                        logger.warning(f"{title}创建文件夹失败: {part}，将使用当前目录")
                    else:
                        folder_id = folder["data"]["Info"]["FileId"]
                        folder_cache[cache_key] = folder_id
                        parent_id = folder_id
                    # time.sleep(1/get_int_env("ENV_FILE_PER_SECOND", 5))  # 避免限流

                # 处理ETag
                if uses_v2_etag:
                    # 实现Base62 ETag转Hex（参考123pan_bot中的实现）
                    etag = optimized_etag_to_hex(etag, True)

                # 秒传文件（带重试）
                retry_count = 3
                rapid_resp = None
                while retry_count > 0:
                    # 检查etag是否与上一个成功转存的文件相同
                    if last_etag == etag:
                        skip_count += 1
                        logger.info(f"{title}跳过重复文件: {file_path}")
                        rapid_resp = {"data": {"Reuse": True, "Skip": True}, "code": 0}  # 标记为跳过
                        break

                    try:
                        rapid_resp = client123.upload_file_fast(
                            file_name=file_name,
                            parent_id=parent_id,
                            file_md5=robust_normalize_md5(etag),
                            file_size=size,
                            duplicate=1
                        )
                        check_response(rapid_resp)
                        break
                    except Exception as e:
                        retry_count -= 1
                        logger.warning(f"{title}转存文件 {file_name} 失败 (剩余重试: {retry_count}): {str(e)}")
                        if rapid_resp and ("同名文件" in rapid_resp.get("message", {})):
                            notifier.send_message(f"{title}" + rapid_resp.get("message", {}))
                        if rapid_resp and ("Etag" in rapid_resp.get("message", {})):
                            break
                        if rapid_resp and ("文件信息" in rapid_resp.get("message", {})):
                            notifier.send_message(f"{title}请检查189的Cookie是否过期，或是否添加- NO_PROXY=*.189.cn")
                            break
                        time.sleep(31)

                if rapid_resp is None:
                    # 处理所有重试失败且 rapid_resp 为 None 的场景
                    error_msg = "秒传失败：接口返回空值且重试耗尽"
                    results.append({
                        "success": False,
                        "file_name": file_path,
                        "error": error_msg
                    })
                    error_files.append(file_info)
                    dir_path, file_name = os.path.split(file_path)
                    msg = {
                        'status': '❌',
                        'dir': dir_path,
                        'file': f"{file_name} ({error_msg})"
                    }
                    message_batch.append(msg)
                    batch_size += 1
                    logger.error(f"{title}{msg['status']}:{msg['dir']}/{msg['file']}")
                elif rapid_resp.get("code") == 0 and rapid_resp.get("data", {}) and rapid_resp.get("data", {}).get(
                        "Reuse", False):
                    # 检查是否是跳过的文件
                    if rapid_resp.get("data", {}).get("Skip"):
                        # 解析路径结构
                        dir_path, file_name = os.path.split(file_path)
                        msg = {
                            'status': '🔄',
                            'dir': dir_path,
                            'file': f"{file_name} (重复跳过)"
                        }
                        message_batch.append(msg)
                        batch_size += 1
                        logger.info(f"{title}{msg['status']}:{msg['dir']}/{msg['file']}")
                    else:
                        # 更新上一个成功转存文件的etag
                        last_etag = etag
                        results.append({
                            "success": True,
                            "file_name": file_path,
                            "file_id": rapid_resp.get("data", {}).get("FileId", ""),
                            "size": size
                        })
                        total_size += size
                        # 解析路径结构
                        dir_path, file_name = os.path.split(file_path)
                        msg = {
                            'status': '✅',
                            'dir': dir_path,
                            'file': file_name
                        }
                        message_batch.append(msg)
                        batch_size += 1
                        logger.info(f"{title}{msg['status']}:{msg['dir']}/{msg['file']}")

                else:
                    results.append({
                        "success": False,
                        "file_name": file_path,
                        "error": "此文件在123服务器不存在，无法秒传" if rapid_resp.get("data", {}) and (
                                    rapid_resp.get("data", {}).get("Reuse", True) == False) else rapid_resp.get(
                            "message", "未知错误")
                    })
                    error_files.append(file_info)
                    # 解析路径结构
                    dir_path, file_name = os.path.split(file_path)
                    msg = {
                        'status': '❌',
                        'dir': dir_path,
                        'file': f"{file_name} ({"此文件在123服务器不存在，无法秒传" if rapid_resp.get("data", {}) and (rapid_resp.get("data", {}).get("Reuse", True) == False) else rapid_resp.get("message", "未知错误")})"
                    }
                    message_batch.append(msg)
                    batch_size += 1
                    logger.info(f"{title}{msg['status']}:{msg['dir']}/{msg['file']}")

                # 每10条消息发送一次
                if batch_size % 10 == 0:
                    # 生成树状结构消息
                    tree_messages = defaultdict(lambda: {'✅': [], '❌': [], '🔄': []})
                    for entry in message_batch:
                        tree_messages[entry['dir']][entry['status']].append(entry['file'])

                    batch_msg = []
                    for dir_path, status_files in tree_messages.items():
                        for status, files in status_files.items():
                            if files:
                                batch_msg.append(f"--- {status} {dir_path}")
                                for i, file in enumerate(files):
                                    prefix = '      └──' if i == len(files) - 1 else '      ├──'
                                    batch_msg.append(f"{prefix} {file}")
                    batch_msg = "\n".join(batch_msg)
                    logger.info(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")
                    # notifier.send_message(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")
                    message_batch = []
                time.sleep(1 / get_int_env("ENV_FILE_PER_SECOND", 5))  # 避免限流

            except Exception as e:
                # 解析路径结构
                dir_path, file_name = os.path.split(file_path)
                msg = {
                    'status': '❌',
                    'dir': dir_path,
                    'file': f"{file_name} ({str(e)})"
                }
                message_batch.append(msg)
                batch_size += 1
                logger.info(f"{title}{msg['status']}:{msg['dir']}/{msg['file']}")
                results.append({
                    "success": False,
                    "file_name": file_path,
                    "error": str(e)
                })
                error_files.append(file_info)
                # 每10条消息发送一次
                if batch_size % 10 == 0:
                    # 生成树状结构消息
                    tree_messages = defaultdict(lambda: {'✅': [], '❌': [], '🔄': []})
                    for entry in message_batch:
                        tree_messages[entry['dir']][entry['status']].append(entry['file'])

                    batch_msg = []
                    for dir_path, status_files in tree_messages.items():
                        for status, files in status_files.items():
                            if files:
                                batch_msg.append(f"--- {status} {dir_path}")
                                for i, file in enumerate(files):
                                    prefix = '      └──' if i == len(files) - 1 else '      ├──'
                                    batch_msg.append(f"{prefix} {file}")
                    batch_msg = "\n".join(batch_msg)
                    logger.error(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")
                    # notifier.send_message(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")
                    message_batch = []
                time.sleep(1 / get_int_env("ENV_FILE_PER_SECOND", 5))  # 避免限流

        # 发送剩余的消息
        if message_batch:
            # 生成树状结构消息
            tree_messages = defaultdict(lambda: {'✅': [], '❌': [], '🔄': []})
            for entry in message_batch:
                tree_messages[entry['dir']][entry['status']].append(entry['file'])

            batch_msg = []
            for dir_path, status_files in tree_messages.items():
                for status, files in status_files.items():
                    if files:
                        batch_msg.append(f"--- {status} {dir_path}")
                        for i, file in enumerate(files):
                            prefix = '      └──' if i == len(files) - 1 else '      ├──'
                            batch_msg.append(f"{prefix} {file}")
            batch_msg = "\n".join(batch_msg)
            logger.info(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")
            # notifier.send_message(f"{title}📊 {batch_size}/{total_files_count} ({int(batch_size / total_files_count * 100)}%) 个文件已处理\n\n{batch_msg}")

        # 结束计时并计算耗时
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # 发送转存结果
        success_count = sum(1 for r in results if r['success'])
        fail_count = len(error_files)

        # 将字节转换为GB (1GB = 1024^3 B)
        total_size_gb = total_size / (1024 ** 3)
        size_str = f"{total_size_gb:.2f}GB"

        # 处理JSON文件中的总体积
        total_size_json_gb = total_size_json / (1024 ** 3)
        total_size_json_str = f"{total_size_json_gb:.2f}GB"

        # 计算平均文件大小
        avg_size = total_size / success_count if success_count > 0 else 0
        avg_size_gb = avg_size / (1024 ** 3)
        avg_size_str = f"{avg_size_gb:.2f}GB" if avg_size_gb >= 0.01 else f"{avg_size / (1024 ** 2):.2f}MB"
        # 添加跳过的重复文件数量显示
        result_msg = f"{title}✅ 123转存189完成！\n✅成功: {success_count}个\n❌失败: {fail_count}个\n🔄跳过同一目录下的重复文件: {skip_count}个\n📊成功转存体积: {size_str}\n📊平均文件大小: {avg_size_str}\n📝189分享理论文件数: {total_files_count}个\n⏱️耗时: {time_str}"
        notifier.send_message(f"{result_msg}")
        time.sleep(0.5)
        # 添加失败文件详情
        if fail_count > 0:
            failed_files = []
            for result in results:
                if not result["success"]:
                    # 简化文件名显示
                    file_name = result["file_name"]
                    failed_files.append(f"• {file_name}（失败原因：{result['error']}）")
            # 分批发送所有失败文件，每批最多10个
            batch_size = 20

            for idx in range(0, len(failed_files), batch_size):
                batch = failed_files[idx:idx + batch_size]
                batch_msg = title + "❌ 失败文件 (批次 {}/{}):\n".format((idx // batch_size) + 1, (
                            len(failed_files) + batch_size - 1) // batch_size) + "\n".join(batch)
                notifier.send_message(batch_msg)
                time.sleep(0.5)

            return json.dumps(json_data)

        # 进入重试队列
        # if fail_count > 0:
        #     json_data['files'] = error_files
        #     json_data['totalFilesCount'] = len(error_files)
        #     json_data['totalSize'] = sum(f["size"] for f in error_files)
        #     return json.dumps(json_data)

        # 重试完成清除重新信息
        # if not len(error_files) > 0 and retry_num > 0:
        #     return 'clean_retry'

        return None

    except Exception as e:
        logger.error(f"{title}处理189文件失败: {str(e)}")
        notifier.send_message(f"{title}❌ 处理189文件失败:\n{str(e)}")
        return json.dumps(json_data)

if __name__ == '__main__':
    
    #save_189_link(client, "https://cloud.189.cn/t/3QnUbejaMvui", ENV_189_UPLOAD_PID)
    #save_189_link(client, "https://cloud.189.cn/t/iu2MJ3BjqMBj", ENV_189_UPLOAD_PID)
    #tg_189monitor()
    client = Cloud189()
    try:
        logger.info("189正在登录 ...")
        client.login(ENV_189_CLIENT_ID, ENV_189_CLIENT_SECRET)
    except Exception as e:
        logger.error(f"登录出现错误: {e}")
    info = client.getShareInfo("https://cloud.189.cn/t/NZzmYrQjMb6z")
    info = client.getShareInfo("https://cloud.189.cn/t/ZzyYfmeE3uIb")
    logger.info(info)
    save_189_link(client, "https://cloud.189.cn/t/NZzmYrQjMb6z", 923961206742226023)
    #client.delete_folder_contents("724071207997330113")
    #client.empty_recycle_bin()
    exit(-1)
