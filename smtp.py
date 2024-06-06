import asyncio
from aiosmtpd.controller import Controller

class CustomSMTPHandler:
    async def handle_DATA(self, server, session, envelope):
        print(f'Received email from: {envelope.mail_from}')
        print(f'Recipient(s): {envelope.rcpt_tos}')
        print(f'Email data:\n{envelope.content.decode("utf8", errors="replace")}')
        return '250 Message accepted for delivery'

if __name__ == "__main__":
    handler = CustomSMTPHandler()
    controller = Controller(handler, hostname='localhost', port=1025)
    controller.start()
    print("SMTP server running on localhost:1025")
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        controller.stop()
