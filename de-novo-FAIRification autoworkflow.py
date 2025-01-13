import os
import time
import subprocess
import http.client
import base64
import ssl
import psutil
import jaydebeapi

# AllegroGraph 配置信息
base_url = ""
repository = ""
username = ""
password = ""

# 本地 RDF 文件路径
rdf_file_path = ""

# HTTP API Endpoint
endpoint = f"/repositories/{repository}/statements"

# 使用 TLS 1.2 协议
context = ssl.create_default_context()
context.set_ciphers('TLSv1.2')

# H2数据库配置
jdbc_driver = "org.h2.Driver"
jdbc_url = ""
jdbc_user = ""
jdbc_password = ""
h2_jar_path = ""


def copy_unprocessed_data():
    conn = None
    try:
        # 连接到 H2 数据库
        conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [jdbc_user, jdbc_password], h2_jar_path)
        cursor = conn.cursor()

        # 查询 allpatientinfo 表中的 IS_PROCESSED 为 FALSE 的记录
        cursor.execute("SELECT * FROM ALLPATIENTINFO WHERE IS_PROCESSED = FALSE")
        unprocessed_data = cursor.fetchall()  # 获取未处理的数据

        # 如果没有未处理数据，则直接返回
        if not unprocessed_data:
            print("No unprocessed data found in ALLPATIENTINFO,skip materialize operation")
            return False

        # 将未处理的数据复制到 patientinfo 表
        for row in unprocessed_data:
            cursor.execute("""
                INSERT INTO PATIENTINFO (ID, NAME, AGE, GENDER, BLOODTYPE, DISEASE, DOCTOR, HOSPITAL, UPDATED_AT, IS_PROCESSED)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)  # 将每行数据插入到 patientinfo 表中
        conn.commit()  # 提交事务
        print(f"Find {len(unprocessed_data)} unprocessed records, Copied {len(unprocessed_data)} unprocessed records from ALLPATIENTINFO to PATIENTINFO.")

        return unprocessed_data  # 返回复制的数据，后续用于上传和删除

    except jaydebeapi.DatabaseError as e:
        print(f"数据库操作错误: {e}")
        return False
    except Exception as e:
        print(f"发生错误: {e}")
        return False
    finally:
        # 确保数据库连接被关闭
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"关闭连接时发生错误: {e}")


def run_ontop_materialize():
    # 执行 materialize 操作
    ontop_dir = "C:\\ontop\\ontop-cli-5.2.1"
    command = [
        "C:\\ontop\\ontop-cli-5.2.1\\ontop.bat",  # 使用 ontop.bat 执行命令
        "materialize",
        "-m", "mappingv2.ttl",  # 输入的映射文件
        "-o", "CLI_materialize_output",  # 输出文件夹
        "-p", "ontop.properties"  # 配置文件
    ]

    # 执行命令
    result = subprocess.run(
        command,
        cwd=ontop_dir,  # 设置工作目录
        check=True,  # 确保命令成功执行
        text=True,  # 捕获输出并返回为字符串
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # 打印输出结果
    print(f"输出:\n{result.stdout}")
    print(f"错误信息:\n{result.stderr}")
    print("materialize success")


def upload_rdf():
    print("Check if a file exists...")
    if os.path.exists(rdf_file_path):
        try:
            # 读取文件内容到内存并立即关闭文件
            with open(rdf_file_path, 'rb') as rdf_file:
                rdf_data = rdf_file.read()
            print(f"文件 '{rdf_file_path}' Successfully read into memory。")

            # 设置请求头
            headers = {
                "Content-Type": "application/rdf+xml",
                "Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode("utf-8")
            }

            # 创建 HTTPS 连接并上传文件
            conn = http.client.HTTPSConnection(base_url, context=context)
            conn.request("POST", endpoint, body=rdf_data, headers=headers)
            response = conn.getresponse()

            # 打印响应状态和消息
            print(f"响应状态码: {response.status}, 响应消息: {response.reason}")

            if response.status == 200:
                print(f"RDF 文件 '{rdf_file_path}' Upload Successfully！")
                time.sleep(10)  # 等待，确保操作完成
                return True  # 返回 True，表示上传成功
            else:
                print(f"上传失败：{response.status}, {response.reason}")
                print("详细错误信息：", response.read().decode("utf-8"))
                return False  # 上传失败

        except Exception as e:
            print(f"上传过程中发生错误: {e}")
            return False  # 上传失败
    else:
        print(f"文件 '{rdf_file_path}' Does not exist！")
        return False  # 文件不存在，上传失败


def update_allpatientinfo_and_delete_patientinfo(unprocessed_data):
    conn = None
    try:
        # 连接到 H2 数据库
        conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [jdbc_user, jdbc_password], h2_jar_path)
        cursor = conn.cursor()

        # 更新 allpatientinfo 表中的 IS_PROCESSED 为 TRUE
        cursor.executemany("""
            UPDATE ALLPATIENTINFO 
            SET IS_PROCESSED = TRUE 
            WHERE ID = ?
        """, [(row[0],) for row in unprocessed_data])  # 根据 ID 更新对应记录
        conn.commit()  # 提交事务
        print(f"Updated IS_PROCESSED to TRUE for {len(unprocessed_data)} records in ALLPATIENTINFO.")

        # 删除 patientinfo 表中已复制的数据
        cursor.executemany("""
            DELETE FROM PATIENTINFO 
            WHERE ID = ?
        """, [(row[0],) for row in unprocessed_data])  # 根据 ID 删除 patientinfo 表中的对应记录
        conn.commit()  # 提交事务
        print(f"Deleted {len(unprocessed_data)} records from PATIENTINFO.")

    except jaydebeapi.DatabaseError as e:
        print(f"数据库操作错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 确保数据库连接被关闭
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"关闭连接时发生错误: {e}")


# 主循环：每分钟检查一次并上传
while True:
    # 首先将未处理的数据从 allpatientinfo 表复制到 patientinfo 表
    unprocessed_data = copy_unprocessed_data()
    if unprocessed_data:
        # 执行 materialize 操作
        run_ontop_materialize()

        # 上传 RDF 文件
        if upload_rdf():
            # 如果上传成功，更新 allpatientinfo 表并删除 patientinfo 表中的数据
            update_allpatientinfo_and_delete_patientinfo(unprocessed_data)
    time.sleep(60)  # 每 60 秒执行一次