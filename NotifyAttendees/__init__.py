import logging

import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = msg.get_body().decode('utf-8')
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ["POSTGRES_URL"],
            database=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PW"]
        )

        logging.info('Database connected.')

        cursor = conn.cursor()

        logging.info('Get cursor')

        # TODO: Get notification message and subject from database using the notification_id
        cursor.execute("SELECT n.message, n.subject FROM public.notification n WHERE n.id = %s", (notification_id, ))
        logging.info('Executed')
        notification = cursor.fetchone()

        # TODO: Get attendees email and name
        cursor.execute("SELECT a.email, a.first_name, a.last_name FROM public.attendee a")
        attendees = cursor.fetchall()

        for a in attendees:
            logging.info(a[0])

            # TODO: Loop through each attendee and send an email with a personalized subject
            message = Mail(
                from_email='bruno@brittus.com',
                to_emails='bsb.brunosoares@gmail.com',
                subject=notification[1],
                html_content="%s %s"%(a[0], notification[0])
            )    

            sendEmail(message)
        
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        attendees_notifieds = "Notified %d attendee(s)"%(len(attendees))
        cursor.execute("UPDATE public.notification SET completed_date=%s, status=%s WHERE id = %s", [datetime.utcnow(), attendees_notifieds, notification_id])
        logging.info("Updated")
        conn.commit()
        logging.info("Commited")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')

def sendEmail(message):
    try:
        sg = SendGridAPIClient(os.environ["SENDGRID_KEY"])
        response = sg.send(message)
        logging.info(response.status_code)
        logging.info(response.body)
        logging.info(response.headers)
        
    except Exception as e:
        logging.info(e.message)