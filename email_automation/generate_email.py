import os 
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path

from dotenv import load_dotenv

PORT = 587
EMAIL_SERVER = "emtp-mail.outlook.com" 

#load the enviorment variables / optional 
current_dir = Path(_file).resolve().parent if "__file__" in locals() else Path.cwd() 
envars = current_dir / ".env" 
load_dotenv(envars)


# Put your crentendials below 
sender_email = ""
password_email =""

#Function to send email parameters include Subject, Receiver_Email, Name, Due_date, Invoice_no, amount): 

def send_email(subject, receiver_email, name, due_date, invoice_no, amount ):
    msg = EmailMessage()
    msg["Subject"] = subject 
    msg["From] = formataddr(("Coding is Fun Corp.", f"{sender_email}")) 
    msg["To"] = receiver_email
    msg["BCC"] = sender_email

    msg.set_content(
        f"""\

        Hi {name},

        I hope you are well. 
        I just wanted to drop you a wuick note to remind you that {amount} USD in prespect of our 
        invoice {invoice_no} is due for payment on {due_date}.
        I would be really grateful if you could confirm that everything is on track for payment, 
        Best regards

        Li
        
        """
    )

    msg.add_alternative(

        f"""\ 

        <html> 
            <body>
                <pi> Hi {name}, </p>
                <p> I hope you are well.<p> 
                <p> I hope you are well. </p> 
                <p> I just wanted to drop you a wuick note to remind you that {amount} USD in prespect of our </p> 
                <p> invoice {invoice_no} is due for payment on {due_date}.</p> 
                <p> I would be really grateful if you could confirm that everything is on track for payment,  </p>  
                <p> Best regards </p>
                <p> Li Huang <p/> 
            <body>
        <html>
        """, 
        subtype = "html",
        
    )
    
    
#sends email  
with smtplib.SMTP(EMAIL_SERVER, PORT) as server:
    server.starttls()
    server.login(sender_email,password_email)
    sever.sendmail(sender_email, receiver_email, msg.as_string())