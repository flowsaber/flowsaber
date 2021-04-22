"""
Modified from https://github.com/PrefectHQ/prefect/blob/05cac2372c57a93ea72b05e7c844b1e115c01047/src
/prefect/tasks/notifications/email_task.py#L11
"""

import smtplib
import ssl
from email.message import EmailMessage
from typing import Any

from flowsaber.core.task import Task


class EmailTask(Task):
    """
    Task for sending email from an authenticated email service over SMTP. For this task to
    function properly, you must have the `"EMAIL_USERNAME"` and `"EMAIL_PASSWORD"` Prefect
    Secrets set.  It is recommended you use a [Google App
    Password](https://support.google.com/accounts/answer/185833) if you use Gmail.  The default
    SMTP server is set to the Gmail SMTP server on port 465 (SMTP-over-SSL). Sending messages
    containing HTML code is supported - the default MIME type is set to the text/html.
    Args:
        - subject (str, optional): the subject of the email; can also be provided at runtime
        - msg (str, optional): the contents of the email, added as html; can be used in
            combination of msg_plain; can also be provided at runtime
        - email_to (str, optional): the destination email address to send the message to; can also
            be provided at runtime
        - email_from (str, optional): the email address to send from; defaults to
            notifications@prefect.io
        - smtp_server (str, optional): the hostname of the SMTP server; defaults to smtp.gmail.com
        - smtp_port (int, optional): the port number of the SMTP server; defaults to 465
        - smtp_type (str, optional): either SSL or STARTTLS; defaults to SSL
        - msg_plain (str, optional): the contents of the email, added as plain text can be used in
            combination of msg; can also be provided at runtime
        - email_to_cc (str, optional): additional email address to send the message to as cc;
            can also be provided at runtime
        - email_to_bcc (str, optional): additional email address to send the message to as bcc;
            can also be provided at runtime
        - **kwargs (Any, optional): additional keyword arguments to pass to the base Task
            initialization
    """

    def __init__(
            self,
            username: str,
            password: str,
            subject: str = None,
            msg: str = None,
            email_to: str = None,
            email_from: str = "notifications@prefect.io",
            smtp_server: str = "smtp.gmail.com",
            smtp_port: int = 465,
            smtp_type: str = "SSL",
            msg_plain: str = None,
            email_to_cc: str = None,
            email_to_bcc: str = None,
            **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.username = username
        self.password = password
        self.subject = subject
        self.msg = msg
        self.email_to = email_to
        self.email_from = email_from
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_type = smtp_type
        self.msg_plain = msg_plain
        self.email_to_cc = email_to_cc
        self.email_to_bcc = email_to_bcc
        super().__init__(**kwargs)

    def run(self, *args, **kwargs) -> None:
        """
        Run method which sends an email.
        Args:
            Args will not be used.
        Returns:
            - None, Means the _output channel is a END channel
        """

        username = self.username
        password = self.password

        message = EmailMessage()
        message["Subject"] = self.subject
        message["From"] = self.email_from
        message["To"] = self.email_to
        if self.email_to_cc:
            message["Cc"] = self.email_to_cc
        if self.email_to_bcc:
            message["Bcc"] = self.email_to_bcc

        # https://docs.python.org/3/library/email.examples.html
        # If present, first set the plain content of the email. After which the html version is
        # added. This converts the message into a multipart/alternative container, with the
        # original text message as the first part and the new html message as the second part.
        if self.msg_plain:
            message.set_content(self.msg_plain, subtype="plain")
        if self.msg:
            message.add_alternative(self.msg, subtype="html")

        context = ssl.create_default_context()
        if self.smtp_type == "SSL":
            server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context)
        elif self.smtp_type == "STARTTLS":
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls(context=context)
        else:
            raise ValueError(f"{self.smtp_type} is an unsupported value for smtp_type.")

        server.login(username, password)
        try:
            server.send_message(message)
        finally:
            server.quit()
