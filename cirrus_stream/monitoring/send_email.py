import smtplib
import ssl

sender_email = "yousolar.production@gmail.com"
receiver_email = "brett.nelson@yousolar.com"
message = """\
Subject: Streaming OFFLINE\
\
This message is sent from Python via EC2 instance on AWS.\
Please re-start the optclient streaming service,\
as the service has failed as of the minute. """

port = 465  # For SSL
#password = input("Type your password and press enter: ")
password = "kvjv gxcx abyu hkqe"
# Create a secure SSL context
context = ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    server.login("yousolar.production@gmail.com", password)
    print('logged in')
    server.sendmail(sender_email, receiver_email, message)
    # Send email here
