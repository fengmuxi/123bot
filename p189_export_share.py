import logging
import re
import time
import hashlib
import urllib.parse
import urllib.request
import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any


def create_189_rapid_transfer(share_url: str, share_pwd: str = "") -> dict[
                                                                          str, str | list[dict[str, Any]] | int] | None:
    """
    创建189网盘秒传JSON

    Args:
        share_url: 分享链接
        share_pwd: 分享密码

    Returns:
        秒传JSON对象
    """
    try:
        # 支持两种格式: /t/xxx 或 ?code=xxx
        match = re.search(r'/t/([a-zA-Z0-9]+)', share_url)
        if not match:
            match = re.search(r'[?&]code=([a-zA-Z0-9]+)', share_url)
        if not match:
            raise ValueError(
                "无效的189网盘分享链接 (支持格式: https://cloud.189.cn/t/xxx 或 https://cloud.189.cn/web/share?code=xxx)")

        share_code = match.group(1)
        share_id = share_code  # 默认使用share_code

        # 如果有密码，需要先调用checkAccessCode获取真正的share_id
        if share_pwd:
            print(f"[189] 验证访问码...")
            check_url = f"https://cloud.189.cn/api/open/share/checkAccessCode.action?shareCode={share_code}&accessCode={share_pwd}"

            headers = {
                "Accept": "application/json;charset=UTF-8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Referer": "https://cloud.189.cn/web/main/"
            }

            req = urllib.request.Request(check_url, headers=headers)
            try:
                with urllib.request.urlopen(req) as response:
                    check_text = response.read().decode('utf-8')
                    print(f"[189] checkAccessCode响应: {check_text[:200]}")

                    try:
                        check_data = json.loads(check_text)
                        if check_data.get('shareId'):
                            share_id = check_data['shareId']
                            print(f"[189] 从checkAccessCode获取到share_id: {share_id}")
                    except json.JSONDecodeError:
                        print("[189] checkAccessCode解析失败，继续使用share_code")
            except Exception as e:
                print(f"[189] checkAccessCode请求失败: {e}")

        # 构建请求参数
        params = {
            "shareCode": share_code,
            "accessCode": share_pwd or ""
        }

        # 添加认证签名
        timestamp = str(int(time.time() * 1000))
        app_key = "600100422"

        sign_data = params.copy()
        sign_data.update({
            "Timestamp": timestamp,
            "AppKey": app_key
        })

        signature = get_189_signature(sign_data)

        query_string = urllib.parse.urlencode(params)
        api_url = f"https://cloud.189.cn/api/open/share/getShareInfoByCodeV2.action?{query_string}"

        print(f"[189] 请求分享信息: {api_url}")
        print(f"[189] 签名参数: timestamp={timestamp}, app_key={app_key}, signature={signature}")

        headers = {
            "Accept": "application/json;charset=UTF-8",
            "Sign-Type": "1",
            "Signature": signature,
            "Timestamp": timestamp,
            "AppKey": app_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "Referer": "https://cloud.189.cn/web/main/"
        }

        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            text = response.read().decode('utf-8')

        print(f"[189] 响应状态: {response.status}")
        print(f"[189] 响应内容: {text}")

        # 尝试解析为JSON或XML
        data = None
        if text.strip().startswith('<'):
            # XML响应
            print("[189] 检测到XML响应，开始解析...")
            data = parse_xml_response(text)
            print(f"[189] XML解析结果: {json.dumps(data, indent=2, ensure_ascii=False)}")
        else:
            # JSON响应 - 修复大整数精度问题
            print("[189] 检测到JSON响应")
            try:
                # 使用正则表达式将大整数ID转换为字符串
                fixed_text = re.sub(r'"id":"?(\d{15,})"?', r'"id":"\1"', text)
                fixed_text = re.sub(r'"fileId":"?(\d{15,})"?', r'"fileId":"\1"', fixed_text)
                fixed_text = re.sub(r'"parentId":"?(\d{15,})"?', r'"parentId":"\1"', fixed_text)
                fixed_text = re.sub(r'"shareId":"?(\d{15,})"?', r'"shareId":"\1"', fixed_text)
                data = json.loads(fixed_text)
            except json.JSONDecodeError as e:
                print(f"[189] JSON解析失败: {e}")
                data = json.loads(text)  # 回退到普通解析

        if data.get('res_code') != 0:
            if data.get('res_code') == 40401 and not share_pwd:
                raise ValueError("该分享需要提取码，请输入提取码")
            raise ValueError(f"获取189分享信息失败: {data.get('res_message', '未知错误')}")

        # 如果getShareInfoByCodeV2返回了shareId，更新它
        if data.get('shareId') and data['shareId'] != share_code:
            share_id = data['shareId']
            print(f"[189] 从getShareInfoByCodeV2更新share_id: {share_id}")

        file_name = data.get('fileName', '')
        file_id = data.get('fileId', '')
        need_access_code = data.get('needAccessCode', '0')
        is_folder = data.get('isFolder', False)
        share_mode = data.get('shareMode', '0')

        print(
            f"[189] 分享信息: share_id={share_id}, file_id={file_id}, need_access_code={need_access_code}, is_folder={is_folder}, share_mode={share_mode}, share_code={share_code}, share_pwd={share_pwd}")

        if not share_id or not file_id:
            if need_access_code == "1" and not share_pwd:
                raise ValueError("该分享需要提取码，请输入提取码")
            raise ValueError("获取189分享信息失败，可能是分享链接无效或已过期")

        files = get_189_share_files(share_id, file_id, file_id, "", share_mode, share_pwd, share_code, is_folder)

        return {
            "commonPath": file_name,
            "files": files,
            "totalFilesCount": len(files),
            "totalSize": sum(f["size"] for f in files),
        }
    except Exception as e:
        logging.info(f'189链接转123秒传json文件异常=>{str(e)}')
        return None


def get_189_share_files(share_id: str, share_dir_file_id: str, file_id: str, path: str = "",
                        share_mode: str = "0", access_code: str = "", share_code: str = "", is_folder: bool = True) -> List[Dict[str, Any]]:
    """
    递归获取189网盘分享文件列表
    """
    files = []
    page = 1

    while True:
        params = {
            "pageNum": str(page),
            "pageSize": "100",
            "fileId": str(file_id),
            "shareDirFileId": str(share_dir_file_id),
            "isFolder": str(is_folder),
            "shareId": str(share_id),
            "shareMode": share_mode,
            "iconOption": "5",
            "orderBy": "lastOpTime",
            "descending": "true",
            "accessCode": access_code or ""
        }

        query_string = urllib.parse.urlencode(params)
        url = f"https://cloud.189.cn/api/open/share/listShareDir.action?{query_string}"
        print(
            f'[189] 请求文件列表: page={page}, file_id={file_id}, share_dir_file_id={share_dir_file_id}, path="{path}"')

        # 构建Cookie，包含分享码和访问码的映射
        cookies = []
        if share_code and access_code:
            cookies.append(f'share_{share_code}={access_code}')

        headers = {
            'Accept': 'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Referer': 'https://cloud.189.cn/web/main/'
        }

        if cookies:
            headers['Cookie'] = '; '.join(cookies)

        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req) as response:
                text = response.read().decode('utf-8')
        except Exception as e:
            print(f"[189] 请求失败: {e}")
            break

        print(f'[189] 响应状态: {response.status}, 内容: {text}')

        if response.status != 200:
            print(f"[189] API错误: {response.status} {response.reason}")
            break

        data = None
        try:
            # 使用正则表达式将大整数ID转换为字符串，避免精度丢失
            fixed_text = re.sub(r'"id":(\d{15,})', r'"id":"\1"', text)
            fixed_text = re.sub(r'"fileId":"?(\d{15,})"?', r'"fileId":"\1"', fixed_text)
            fixed_text = re.sub(r'"parentId":(\d{15,})', r'"parentId":"\1"', fixed_text)
            fixed_text = re.sub(r'"shareId":(\d{15,})', r'"shareId":"\1"', fixed_text)
            data = json.loads(fixed_text)
        except json.JSONDecodeError as e:
            print(f"[189] JSON解析失败: {text}")
            break

        if data.get('res_code') != 0:
            print(f"[189] API返回错误: res_code={data.get('res_code')}, message={data.get('res_message', '未知')}")

            # 如果是FileNotFound错误，并且是在子文件夹中，给出提示
            if data.get('res_code') == "FileNotFound" and path:
                print(f"[189] 警告：子文件夹 \"{path}\" 访问失败，189网盘分享可能需要登录才能访问子文件夹")
            break

        file_list_ao = data.get('fileListAO', {})
        file_list = file_list_ao.get('fileList', [])
        folder_list = file_list_ao.get('folderList', [])
        count = file_list_ao.get('count', 0)

        print(f"[189] 找到: {len(file_list)}个文件, {len(folder_list)}个文件夹, count={count}")

        for file in file_list:
            file_path = f"{path}/{file['name']}" if path else file['name']
            files.append({
                "path": file_path,
                "etag": (file.get('md5', '')).lower(),  # MD5转小写
                "size": file.get('size', 0)
            })
            print(f"[189] 添加文件: {file_path} ({file.get('size', 0)} bytes, MD5: {file.get('md5', '')})")

        for folder in folder_list:
            folder_path = f"{path}/{folder['name']}" if path else folder['name']
            print(
                f"[189] 准备进入子文件夹: \"{folder_path}\", id={folder.get('id')}, parentId={folder.get('parentId')}")

            # 进入子目录时，file_id 和 share_dir_file_id 都使用子文件夹的 id
            sub_files = get_189_share_files(
                share_id,
                folder['id'],  # share_dir_file_id 使用子文件夹的 id
                folder['id'],  # file_id 也使用子文件夹的 id
                folder_path,
                share_mode,
                access_code,
                share_code
            )
            print(f"[189] 子文件夹 \"{folder_path}\" 返回了 {len(sub_files)} 个文件")
            files.extend(sub_files)

        # 判断是否需要继续分页：
        # 1. 如果 file_list 和 folder_list 都为空，说明没有更多数据
        # 2. 如果返回的总数量小于 pageSize，说明这是最后一页
        total_items = len(file_list) + len(folder_list)
        if total_items == 0 or total_items < 100:
            break

        page += 1

    print(f'[189] 完成文件夹 "{path}": 共{len(files)}个文件')
    return files


def parse_xml_response(xml_text: str) -> Dict[str, Any]:
    """
    解析XML响应
    """
    print("[189] 开始解析XML...")

    def get_tag_value(xml, tag_name):
        pattern = f"<{tag_name}>([^<]*)</{tag_name}>"
        match = re.search(pattern, xml, re.IGNORECASE)
        return match.group(1) if match else None

    res_code = int(get_tag_value(xml_text, 'res_code') or '0')
    res_message = get_tag_value(xml_text, 'res_message') or ''
    share_id = get_tag_value(xml_text, 'shareId') or ''
    file_id = get_tag_value(xml_text, 'fileId') or ''
    share_mode = get_tag_value(xml_text, 'shareMode') or '0'
    is_folder = get_tag_value(xml_text, 'isFolder') == 'true'
    need_access_code = get_tag_value(xml_text, 'needAccessCode') or '0'
    file_name = get_tag_value(xml_text, 'fileName') or ''

    parsed = {
        'res_code': res_code,
        'res_message': res_message,
        'shareId': share_id,
        'fileId': file_id,
        'shareMode': share_mode,
        'isFolder': is_folder,
        'needAccessCode': need_access_code,
        'fileName': file_name
    }

    print(f"[189] XML解析完成: {parsed}")
    return parsed


def get_189_signature(params: Dict[str, str]) -> str:
    """
    189网盘签名算法
    """
    # 对参数按key排序并拼接为 key=value 形式
    sorted_keys = sorted(params.keys())
    sorted_params = '&'.join(f"{key}={params[key]}" for key in sorted_keys)

    print(f"[189] 签名字符串: {sorted_params}")

    # 计算MD5
    return simple_md5(sorted_params)


def simple_md5(s: str) -> str:
    """
    MD5实现
    """
    return hashlib.md5(s.encode('utf-8')).hexdigest().lower()


# 使用示例
if __name__ == "__main__":
    try:
        # 示例用法
        # share_url = "https://cloud.189.cn/t/eqEFJvRjuqee"
        share_url = "https://cloud.189.cn/t/RFbYfm67ZzQf"
        share_pwd = ""  # 如果有密码则填写

        result = create_189_rapid_transfer(share_url, share_pwd)
        print("秒传信息获取成功:")
        print(f"json信息:{result}")
        print(f"文件数量: {result['totalFilesCount']}")
        print(f"总大小: {result['totalSize']} bytes")
        print("文件列表:")
        for file in result['files'][:5]:  # 只显示前5个文件
            print(f"  {file['path']} - {file['size']} bytes")

    except Exception as e:
        print(f"错误: {e}")