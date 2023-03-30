import os
import boto3
from botocore.client import Config
from datetime import datetime, timezone
import platform

def set_creation_time(file_path, creation_time, modification_time):
    if platform.system() == 'Windows':
        # Windows 只支持修改文件的创建时间和修改时间
        import pywintypes, win32file, win32con
        # 将 datetime 对象转换为 Windows 文件时间
        creation_time = pywintypes.Time(creation_time)
        modification_time = pywintypes.Time(modification_time)
        # 打开文件以修改其属性
        handle = win32file.CreateFile(file_path, 0x0100, 0, None, win32con.OPEN_EXISTING, 0, None)
        # 设置文件的创建时间和修改时间
        win32file.SetFileTime(handle, creation_time, None, modification_time)
        # 关闭文件句柄
        handle.Close()
    else:
        # Unix 系统支持修改文件的访问时间和修改时间
        os.utime(file_path, (creation_time.timestamp(), modification_time.timestamp()))


def is_directory(remote_path):
    return remote_path.endswith('/')


# 配置你的Vultr Object Storage API凭据
ACCESS_KEY = 'Your_Access_Key'
SECRET_KEY = 'Your_Secret_Key'

# 设置要同步的本地和远程目录
LOCAL_DIRECTORY = r'Your_Local_Folder'
BUCKET_NAME = 'Your_Bucket_Name'
REMOTE_FOLDER = 'Your_Remote_Folder'

# 使用你的Vultr Object Storage凭据创建一个S3客户端
s3 = boto3.client('s3', endpoint_url='Your_Remote_Storage_Url', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, config=Config(signature_version='s3v4'))

# 上传本地文件到Vultr Object Storage
def upload_file(local_path, remote_path):
    s3.upload_file(local_path, BUCKET_NAME, f'{REMOTE_FOLDER}/{remote_path}')

# 下载远程文件到本地
def download_file(remote_path, local_path, remote_creation_time, remote_modification_time):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if not os.path.exists(local_path):
        s3.download_file(BUCKET_NAME, f'{REMOTE_FOLDER}/{remote_path}', local_path)
        set_creation_time(local_path, remote_creation_time, remote_modification_time)



# 检查并同步本地文件夹与远程文件夹
def sync_directories(local_directory, remote_directory):
    # 获取远程文件夹中的文件列表
    remote_objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=REMOTE_FOLDER)
    remote_files = {obj['Key'].replace(REMOTE_FOLDER + '/', ''): obj['LastModified'] for obj in remote_objects.get('Contents', [])}

    local_files = set()

    # 遍历本地文件夹
    for root, _, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            remote_path = os.path.relpath(local_path, local_directory).replace('\\', '/')
            local_mtime = datetime.fromtimestamp(os.path.getmtime(local_path), timezone.utc)

            local_files.add(remote_path)

            # 如果本地文件在远程文件夹中不存在，或者文件的最后修改时间不同，则上传文件
            if remote_path not in remote_files:
                print(f"{local_path} not found in remote storage. Uploading...")
                upload_file(local_path, remote_path)
            elif local_mtime > remote_files[remote_path]:
                print(f"File {local_path} modified: local = {local_mtime}, remote = {remote_files[remote_path]}. Uploading...")
                upload_file(local_path, remote_path)

    # 检查云端文件是否在本地存在
    # 检查云端文件是否在本地存在
    for remote_path in remote_files:
        local_path = os.path.join(local_directory, remote_path)

        if is_directory(remote_path):
            # 如果远程路径是一个目录，则在本地创建该目录
            os.makedirs(local_path, exist_ok=True)
        elif remote_path not in local_files:
            print(f'{remote_path} not found in local storage. Downloading...')
            # 获取远程文件的元数据
            remote_metadata = s3.head_object(Bucket=BUCKET_NAME, Key=f'{REMOTE_FOLDER}/{remote_path}')
        
            # 获取远程文件的创建时间和最后修改时间
            remote_creation_time = remote_metadata['LastModified'].replace(tzinfo=timezone.utc).astimezone(tz=None)
            remote_modification_time = remote_creation_time  # 假设创建时间和最后修改时间相同

            download_file(remote_path, local_path, remote_creation_time, remote_modification_time)



if __name__ == '__main__':
    sync_directories(LOCAL_DIRECTORY, REMOTE_FOLDER)
