import multiprocessing as mp
import os
from subprocess import PIPE, Popen
import fnmatch
import sqlite3


path = 'hdrs'
num_processes = mp.cpu_count()

connection=sqlite3.connect('DGP.db')
c=connection.cursor()
c.execute('PRAGMA journal_mode=wal')

def cmdline(command):
    process = Popen(
        args=command,
        stdout=PIPE,
        shell=True
    )
    return process.communicate()[0]


def taskEvalglare(file):
    evaluate(file)


def evaluate(file):

    filename = str(os.path.basename(file).split('.hdr')[0])

    if not checkDB(filename):

        if fnmatch.fnmatch(file, '*.hdr'):

            res = cmdline('evalglare -d ' + file + ' | tail -n1 ')

            if res !=b'':
                dgp = (((res).decode("utf-8")).split(':')[1]).split(' ')[1]
                ev=  (((res).decode("utf-8")).split(':')[1]).split(' ')[3]
                connection = sqlite3.connect('DGP.db')
                c2 = connection.cursor()
                try:
                    c2.execute("INSERT INTO id_dgp_ev VALUES (?,?,?)",(filename,dgp,ev))
                    connection.commit()
                except sqlite3.OperationalError:
                    pass

                connection.close()

def checkDB(filename):

    connection = sqlite3.connect('DGP.db')
    c1 = connection.cursor()
    c1.execute('PRAGMA journal_mode=wal')
    try:
        c1.execute("CREATE TABLE IF NOT EXISTS id_dgp_ev (ID PRIMARY KEY ,DGP,EV)")

        c1.execute("SELECT ID FROM id_dgp_ev WHERE ID=?", (filename,))
        exists = c1.fetchall()
        connection.commit()
    except sqlite3.OperationalError:
        pass
    connection.close()
    if exists:
        return True
    else:
        return False




c.execute("SELECT PATH FROM id_path ")
paths=c.fetchall()
connection.close()

pool = mp.Pool(processes=num_processes)
pool.map(taskEvalglare, [elt[0] for elt in paths])
pool.close()
pool.join()