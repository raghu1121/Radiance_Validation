import os
import glob
import shutil
import multiprocessing as mp

views=['v1','v2','v3','v4']
years=['2014','2015','2016','2017']

newfolder='movedHDRs'
Ecfolder='hdrs/EC'

# for view in views:
#     for year in years:
#         for month in range(1,13,1):
#             try: os.makedirs(os.path.join(newfolder,view,year,str(month)))
#             except OSError:
#                 pass
#             files=glob.glob(Ecfolder+'/'+view+'_'+year+ '_'+ str(month)+ '_*')
#             for file in files:
#                 shutil.move(file,os.path.join(newfolder,view,year,str(month)))




def taskmover(view, year,month):
    try:
        os.makedirs(os.path.join(newfolder, view, year, str(month)))
    except OSError:
        pass
    files = glob.glob(Ecfolder + '/' + view + '_' + year + '_' + str(month) + '_*')
    for file in files:
        if not os.path.getsize(file) < 500000:
            shutil.move(file, os.path.join(newfolder, view, year, str(month)))

def func(args):
    return taskmover(*args)
job_args=[]
pool=mp.Pool(processes=4)
for view in views:
    for year in years:
        for month in range(1,13,1):
            comb=[view,year,month]
            job_args.append(comb)
pool.map(func,job_args)
pool.close()
pool.join()
