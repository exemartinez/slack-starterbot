import os
import time
import logging
import sqlite3 as sqlite
from slackclient import SlackClient

class LogMngr():

    logger = None

    '''
    Constructor initializes the base logging configuration.
    '''
    def __init__(self, logger_name):

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        #fh = logging.StreamHandler() #this is to print it in the common console.
        fh = logging.FileHandler('importing_database_data.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    '''
    Prints INFO kind of data.
    '''
    def info(self, valor):
        self.logger.info(valor)

    '''
    Prints ERRORS kind of data.
    '''
    def error(self, valor):
        self.logger.info(valor)

    '''
    Prints DEBUG kind of data.
    '''
    def debug(self, valor):
        self.logger.debug(valor)

log_bot = LogMngr("bot_behavior")

# starterbot's ID as an environment variable
BOT_ID = "U3KP2424A"
SLACK_BOT_TOKEN = "xoxb-121784138146-M1dNUS6OFodBTcPyMxh6vald"
# constants
AT_BOT = "<@" + BOT_ID + ">"
EXAMPLE_COMMAND = "do"
NUEVA_PREGUNTA = "/nueva_pregunta:"
GET_ALL_PREGUNTAS = "/todas_preguntas:"

# instantiate Slack & Twilio clients
slack_client = SlackClient(SLACK_BOT_TOKEN)


def handle_command(command, channel):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    response = "Not sure what you mean. Use the *" + EXAMPLE_COMMAND + \
               "* command with numbers, delimited by spaces."

    if command.startswith(EXAMPLE_COMMAND):
        response = "Sure...write some more code then I can do that!"

    elif command.startswith(NUEVA_PREGUNTA):
        log_bot.info(str(command))

        pregunta_nueva = str(command[len(NUEVA_PREGUNTA)::])
        dao = DAO()

        dao.open_connection()
        dao.exec_new_single_question(pregunta_nueva, "Unitrade")
        dao.close_connection()

        response = "Agregada la pregunta a la base: " + pregunta_nueva

    elif command.startswith(GET_ALL_PREGUNTAS):
        log_bot.info(str(command))

        dao = DAO()

        dao.open_connection()
        preguntas = dao.exec_get_all_questions("Unitrade")

        log_bot.info(str(preguntas))

        channel_info = slack_client.api_call(
          "channels.info",
          channel=channel
        )

        log_bot.info(str(channel_info))

        dao.close_connection()

        response = "Preguntas actuales: " + str(preguntas)

    slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)


def parse_slack_output(slack_rtm_output):
    """
        The Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
    """
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip().lower(), \
                       output['channel']
    return None, None

'''Database access and edition of the questions and answers to the bot.'''
class DAO (object):

    SQLITE_CONSTRAINT_UNIQUE = 2067
    SQLITE_CONSTRAINT = 19
    SQLITE_ERROR = 1
    SQLITE_OK = 0

    log = LogMngr("database_transactions")

    '''
    Updates a contact with new data into the database
    '''
    def exec_upd_single_contacto(self, contacto):

        try:

            result = self.cursor.execute("update contactos set nombre = ?, apellido = ?, email = ?, compania = ?, posicion = ?, tipo = ? where email = ?", (contacto.getNombre(), contacto.getApellido(), contacto.getEmail(), contacto.getCompania(), contacto.getPosicion(), contacto.getTipo(), contacto.getEmail()))
            self.connection.commit()

        except sqlite.IntegrityError as er:
            self.log.error("Database Error: " + str(er.args))
            if (str(er.message)[:6]=="UNIQUE"):
                return self.SQLITE_CONSTRAINT_UNIQUE #Unique constraint failed.
            else:
                return self.SQLITE_CONSTRAINT

        except sqlite.Error as er:
            self.log.error("Database Error: " + er.message)
            self.log.error("The database insert failed with contacto: " + contacto.getNombre() + " " + contacto.getApellido())
            self.connection.rollback()
            return self.SQLITE_ERROR #SQLite error or missing database

        return self.SQLITE_OK #sqlite OK! yey!

    '''
    Inserts a new pregunta in the database.
    '''
    def exec_new_single_question(self, pregunta, team):

        try:

            result = self.cursor.execute("insert into preguntas (pregunta, team) values (?, ?)", (pregunta, team,))
            self.connection.commit()

        except sqlite.IntegrityError as er:
            self.log.error("Database Error: " + str(er.args))
            if (str(er.message)[:6]=="UNIQUE"):
                return self.SQLITE_CONSTRAINT_UNIQUE #Unique constraint failed.
            else:
                return self.SQLITE_CONSTRAINT

        except sqlite.Error as er:
            self.log.error("Database Error: " + er.message)
            self.log.error("The database insert failed with pregunta: " + pregunta + " for team " + team)
            self.connection.rollback()
            return 1 #SQLite error or missing database

        return 0 #sqlite OK! yey!

    def exec_delete_tipo_contacto(self, tipo):
        '''
        Deletes all the contacto's records with the given tipo.
        '''
        try:

            self.cursor.execute("delete from contactos where tipo=?", (str(tipo),))
            self.connection.commit()

        except:
            self.log.error("The database deletion by TIPO failed with TIPO: " + str(tipo))
            self.connection.rollback()
            return False

        return True

    def exec_get_contacto_exists_byCompania(self, compania):
        '''
        Returns one, single sub categoria by its URL
        '''
        self.cursor.execute("select * from contactos where compania=?", (compania,))
        return self.cursor.fetchone()

    def exec_get_all_questions(self, team):
        '''returns all the preguntas in the database for the given team'''

        self.cursor.execute("select * from preguntas where team=?", (team,))
        result = self.cursor.fetchall()

        self.log.debug("search ---- " + str(result))
        return result

    def exec_get_all_contactos(self):
        '''
        Returns every categoria that is in place (full scan)
        '''
        self.log.debug("Fetching...")
        self.cursor.execute("select id, nombre, apellido, email, compania, posicion, tipo from contactos")

        return self.cursor.fetchall()

    def open_connection(self):
        '''
        Sets the due connections to the data stores.
        '''

        self.log.debug("Initiating database")
        # Initializes the connection to SQLite (and creates the due tables)
        self.connection = sqlite.connect('./preguntasRespuestas.db')
        self.connection.text_factory = str

        self.cursor = self.connection.cursor()

        #Creates the database TABLES, if there is NONE

        self.cursor.execute('CREATE TABLE IF NOT EXISTS preguntas ' \
                    '(id INTEGER PRIMARY KEY, pregunta varchar(140), team varchar(140), usuario varchar(140))')

        self.log.debug("Database, READY to throw operations at her.")

    def close_connection(self):
        '''
        Closes the database connection to avoid issues related to the connectivity.
        '''
        self.log.debug("Database offline.")
        self.cursor.close()


if __name__ == "__main__":
    READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose
    if slack_client.rtm_connect():
        print("StarterBot connected and running!")
        while True:
            command, channel = parse_slack_output(slack_client.rtm_read())
            if command and channel:
                handle_command(command, channel)
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")
