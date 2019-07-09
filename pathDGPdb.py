import multiprocessing as mp
import os
from subprocess import PIPE, Popen
import fnmatch
import sqlite3

num_processes = mp.cpu_count()
path = 'hdrs'
filepaths=[]
connection=sqlite3.connect('DGP.db')
c=connection.cursor()
c.execute('PRAGMA journal_mode=wal')
c.execute("CREATE TABLE IF NOT EXISTS id_path (ID PRIMARY KEY ,PATH)")
connection.commit()
connection.close()

def taskPaths(names):
    insert_db(names)


def insert_db(filepath):
    filename = str(os.path.basename(filepath).split('.hdr')[0])
    # print(filename)
    connection = sqlite3.connect('DGP.db')
    c = connection.cursor()
    c.execute('PRAGMA journal_mode=wal')
    try:
        c.execute("INSERT OR IGNORE INTO  id_path VALUES (?,?)", (filename, filepath))
        connection.commit()
    except sqlite3.OperationalError:
        pass
    connection.close()

for root, d_names, f_names in os.walk(path):
    for f in f_names:
        filepath = os.path.join(root, f)
        size = os.path.getsize(filepath)
        if size > 500000:
            filepaths.append(filepath)

pool = mp.Pool(processes=num_processes)
pool.map(taskPaths, [name for name in filepaths])
pool.close()
pool.join()
connection.close()