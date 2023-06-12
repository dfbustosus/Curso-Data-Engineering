from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from key import PASSWORD_EMAIL


def send_email():
    try:
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587

        # Credenciales de inicio de sesión
        sender_email = 'arnifuentes29@gmail.com'
        password = PASSWORD_EMAIL

        # Configuración del correo electrónico
        subject = 'Carga de datos'
        # body_text = str(df_parameter.iloc[0].tolist())
        body_text = 'Los datos extraídos de la API fueron cargados a la base de datos exitosamente.'

        # Crear objeto MIMEMultipart y agregar partes
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body_text, 'plain'))

        # Iniciar conexión SMTP y enviar correo electrónico
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)

        # Imprimir mensaje de éxito
        print('Email enviado.')

    except Exception as exception:
        print(exception)
        print('Email no ha sido enviado.')
        print('Failure')
        