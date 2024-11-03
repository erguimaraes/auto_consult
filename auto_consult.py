import mysql.connector
import pandas as pd
import schedule
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText

def status():
    try:
        conn = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )
        print("Conexão com o banco de dados estabelecida.")

        query = '''SELECT DISTINCT atendente, resposta, tma, problema, status
        FROM tb_sac
        WHERE atendente IN ('Diego Amorim', 'João Paulo', 'Carol Espinosa')'''
        
        df = pd.read_sql(query, conn)
        print(f"Número de linhas retornadas: {len(df)}")
        
        conn.close()
        return df
    except Exception as e:
        print("Ocorreu um erro ao conectar ao banco de dados:", e)
        return None

def send_email(report_file):
    from_addr = 'xxxxxxxxxx@hotmail.com'  # Substitua pelo seu email
    to_addr = 'xxxxxxxxxxx@gmail.com'  # Substitua pelo email do destinatário
    subject = 'Relatório Diário'
    body = 'Segue em anexo o relatório diário.'

    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    # Anexando o relatório
    try:
        with open(report_file, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={report_file}')
            msg.attach(part)

        # Enviando o email
        with smtplib.SMTP('smtp.office365.com', 587) as server:  # Substitua pelo servidor SMTP
            server.starttls()
            server.login(from_addr, 'senha')  # Substitua pela senha do aplicativo gerada
            server.send_message(msg)
            print(f"Email enviado com sucesso para {to_addr}.")
    except Exception as e:
        print("Ocorreu um erro ao enviar o email:", e)

def job():
    print("Iniciando a tarefa programada...")
    df = status()
    if df is not None and not df.empty:
        print("Dados retornados com sucesso.")
        print(df)  # Exibe o DataFrame no console

        # Salvar o DataFrame em um arquivo CSV
        df.to_csv('relatorio.csv', index=False)  # Salva como CSV
        print("Relatório salvo como 'relatorio.csv'.")

        # Enviar o relatório por email
        send_email('relatorio.csv')
    else:
        print("Nenhum dado retornado ou erro na consulta.")

# Agendar a execução da função job
schedule.every().day.at("09:00").do(job)  # Executa diariamente às 9h

while True:
    schedule.run_pending()
    time.sleep(60)  # Espera um minuto antes de checar novamente
