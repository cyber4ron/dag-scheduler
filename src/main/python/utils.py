# -*- coding:utf-8 -*-

# log
import logging

logger = logging.getLogger("sh-cmd-scheduler")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('../sh-cmd-scheduler.log', mode='w')
fh.setLevel(logging.DEBUG)

# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


# unicode -> ascii
def unc_to_asc(ustr):
    return ustr.decode("utf-8").encode("ascii")


# mail
def send_mail(sub, msg):
    import smtplib
    from email.mime.text import MIMEText

    email_smtp = smtplib.SMTP("xxx.wandoujia.com", 25)

    msg = MIMEText(unc_to_asc(msg))
    msg['Subject'] = sub
    msg['From'] = "xxx@wandoujia.com"
    msg['To'] = "xxx@wandoujia.com"
    email_smtp.sendmail(msg['From'], msg['To'], msg.as_string())

    email_smtp.quit()
