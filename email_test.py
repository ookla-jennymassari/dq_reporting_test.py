import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def send_email():
    #path to the HTML file
    # html_file_path = "dq_reporting.html"

    # # Read the HTML file content
    # try:
    #     with open(html_file_path, "r") as f:
    #         html_content = f.read()
    # except FileNotFoundError:
    #     print(f"HTML file not found: {html_file_path}")
    #     return
    
    # Email content
    email_body = """
    <p>Hello!</p>
    <p>This is a test email.</p>
    <p>Thanks,<br>Jenny</p>
    """

    # Email credentials and SMTP configuration
    email_user = os.getenv("SES_USER")  
    email_password = os.getenv("SES_PWD")  
    smtp_endpoint = "email-smtp.us-west-2.amazonaws.com"

    send_from = "jenny.massari@ookla.com"
    send_to = ["jenny.massari@ookla.com"]

    # Create the email message
    msg = MIMEMultipart("alternative")
    msg['From'] = send_from
    msg['To'] = ", ".join(send_to)
    msg['Subject'] = "Test Email with Custom Message and HTML Report"

    # Attach the custom message
    msg.attach(MIMEText(email_body, 'html'))

    # Attach the HTML file content
    msg.attach(MIMEText(html_content, 'html'))

    try:
        # Connect to the SMTP server
        with smtplib.SMTP(smtp_endpoint, 587) as s:
            s.starttls()  
            s.login(email_user, email_password)  
            s.sendmail(send_from, send_to, msg.as_string())  
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Call the function to send the email
send_email()