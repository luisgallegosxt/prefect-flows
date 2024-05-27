from prefect import task, flow, get_run_logger
import glob
import os
import datetime
import requests
import subprocess
import uuid

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders


@task(log_stdout=True)
def send_mail_operator(host,
						port,
						email_from,attach_path_files_list,
						email_from_pass,
						recipients,
						recipients_cc,
						subject,
						body_text_plain,
						body_html_plain,
						static_sign,
						pathbase
						):
	logger = prefect.context.get("logger")
	logger.info("Start send mail")

	strFrom = email_from
	recipients_cc = recipients_cc
	strPassword = email_from_pass
	
	strSubject = subject
	strHost = host
	intPort = port
	loginFrom = email_from
	strTextPlain = body_text_plain
	static_sign = static_sign
	attach_image = [{'cid':'<image2>','file':static_sign}]
	strTextHtml = body_html_plain
	doBccSender = True

	# Create the root message and fill in the from, to, and subject headers
	msgRoot = MIMEMultipart('related')
	msgRoot['Subject'] = strSubject
	msgRoot['From'] = strFrom
	msgRoot['To'] = ",".join(recipients)
	if doBccSender:
		msgRoot['Bcc'] = strFrom
	msgRoot.preamble = 'This is a multi-part message in MIME format.'

	# Encapsulate the plain and HTML versions of the message body in an
	# 'alternative' part, so message agents can decide which they want to display.
	msgAlternative = MIMEMultipart('alternative')
	msgRoot.attach(msgAlternative)

	msgText = MIMEText(strTextPlain.encode('utf-8'), 'plain', 'UTF-8')
	msgAlternative.attach(msgText)

	# We reference the image in the IMG SRC attribute by the ID we give it below
	msgText = MIMEText(strTextHtml.encode('utf-8'), 'html', 'UTF-8')
	msgAlternative.attach(msgText)

	# Attachments Files Excel
	if attach_path_files_list:
		for attachments in attach_path_files_list:
			part = MIMEBase('application', "octet-stream")
			part.set_payload(open(attachments, 'rb').read())
			encoders.encode_base64(part)
			part.add_header('Content-Disposition', 'attachment; filename={}'.format(attachments.replace(pathbase, '')))
			msgRoot.attach(part)

	# Attachments Images
	for att in attach_image:
		fp = open(att['file'], 'rb')
		msgImage = MIMEImage(fp.read())
		fp.close()
		msgImage.add_header('Content-ID', att['cid'])
		msgRoot.attach(msgImage)

	# Send the email (this example assumes SMTP authentication is required)
	smtp = smtplib.SMTP_SSL(host=strHost, port=intPort)
	smtp.login(loginFrom, strPassword)
	try:
		if doBccSender:
			recipients = recipients + [recipients_cc]
			smtp.sendmail(strFrom, recipients, msgRoot.as_string())
			logger.info('Email sended to: ' + str(recipients))
			
		else:
			smtp.sendmail(strFrom, recipients, msgRoot.as_string())
			
	except smtplib.SMTPRecipientsRefused:
		logger.error('Error sending mail to: ' + str(recipients))
	finally:
		smtp.quit()

@flow
def send_mail():

	# Parameters
	pathbase = Parameter('pathbase', default=None)
	host = Parameter('host')
	port = Parameter('port')
	email_from = Parameter('email_from')
	attach_path_files_list = Parameter('attach_files_path', default=None)
	email_from_pass = Parameter('email_from_pass')
	recipients = Parameter('recipients')
	recipients_cc = Parameter('recipients_cc', default=email_from)
	subject = Parameter('subject')
	body_text_plain = Parameter('body_text_plain')
	body_html = Parameter('body_html')


	static_sign = STATIC_SIGN_FILENAME

	send_mail_operator = send_mail_operator(host,
						port,
						email_from,
						attach_path_files_list,
						email_from_pass,
						recipients,
						recipients_cc,
						subject,
						body_text_plain,
						body_html,
						static_sign,
						pathbase
						)
	
deployment = Deployment.build_from_flow(
    flow = send_mail,
    name = "send_mail",
	path = '/path/to/code/',
	work_queue_name = 'default',
	work_pool_name = 'default-agent-pool',
	tags = ['mailer']
)
deployment.apply()