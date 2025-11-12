import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

def send_email_with_excel(smtp_server, port, sender_email, password, 
                         receiver_email, subject, body, xlsx_file_path):
    """
    发送带Excel附件的邮件
    
    参数:
    smtp_server: SMTP服务器地址
    port: 端口号
    sender_email: 发件人邮箱
    password: 邮箱密码或授权码
    receiver_email: 收件人邮箱
    subject: 邮件主题
    body: 邮件正文
    excel_file_path: Excel文件路径
    """
    
    # 创建邮件对象
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
     # 添加邮件正文
    msg.attach(MIMEText(body, 'plain'))
    
    # 添加xlsx附件
    try:
        with open(xlsx_file_path, 'rb') as file:
            # 读取xlsx文件内容
            xlsx_data = file.read()
            
            # 创建MIMEApplication对象
            attachment = MIMEApplication(xlsx_data, _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', 
                                 filename=os.path.basename(xlsx_file_path))
            
            # 将附件添加到邮件中
            msg.attach(attachment)
            
        print(f"成功添加附件: {os.path.basename(xlsx_file_path)}")
        
    except FileNotFoundError:
        print(f"错误: 文件 {xlsx_file_path} 未找到")
        return False
    except Exception as e:
        print(f"添加附件时出错: {str(e)}")
        return False
        
    
    # 发送邮件
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # 启用安全连接
        server.login(sender_email, password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print("邮件发送成功！")
        return True
        
    except Exception as e:
        print(f"邮件发送失败: {e}")
        return False