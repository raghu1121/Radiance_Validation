import multiprocessing as mp
import csv
import os
import itertools
import glob

num_processes = mp.cpu_count()
processes = []
viewtimes = []
states = []
job_args = []
views = ['v6','v7','v8']

files=glob.glob('weather_stn/kaiserslautern_*.wea')
for file in files:
    tint=str(os.path.basename(file).split('_')[2].split('.')[0])
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for item in itertools.islice(reader, 1, None):
            viewtimes.append(item)


    list = [tint]
    for subset in itertools.product(list, repeat=3):
        states.append(subset)


    def gendaylit(m, d, h, dir, diff, year):
        line = '!gendaylit' + ' ' + m + ' ' + d + ' ' + h + '  -m -15 -o -7.75 -a 49.42 -O 0 -W ' + dir + ' ' + diff
        sky_fname = path('sky', '', year, m, d) + '/sky_' + year + '_' + m + '_' + d + '_' + h + '.rad'
        if not os.path.isfile(sky_fname):
            with open('sky.rad', 'r') as infile, open(sky_fname, 'w') as outfile:
                outfile.write(line)
                outfile.write('\n')
                for item in itertools.islice(infile, 2, None):
                    outfile.write(item)


    def genECHDR(m, d, h, view, state, year):
        zone1 = state[0]
        zone2 = state[1]
        zone3 = state[2]
        # print(zone1,zone2,zone3)
        octree_fname = path('octree', '', year, m,
                            d) + '/' + year + '_' + m + '_' + d + '_' + h + '_' + zone1 + '_' + zone2 + '_' + zone3 + '.oct'
        hdr_fname = path('hdrs', view, year, m,
                         d) + '/' + view + '_' + year + '_' + m + '_' + d + '_' + h + '_' + zone1 + '_' + zone2 + '_' + zone3 + '.hdr'
        if not os.path.isfile(octree_fname):
            octree = 'oconv materials/materials.rad ' + path('sky', '', year, m,
                                                             d) + '/sky_' + year + '_' + m + '_' + d + '_' + h + '.rad Geometry/EC/EC_Bottom_' + zone3 + '.rad Geometry/EC/EC_Middle_' + zone2 + '.rad Geometry/EC/EC_Top_' + zone1 + '.rad Geometry/context.rad Geometry/LiSA_E296_Blinds_Eh..opq.rad >  ' + octree_fname
            os.system(octree)
        if not os.path.isfile(hdr_fname):
            rpict = 'rpict -vf views/' + view + '.vf -x 800 -y 800 -vv 180 -vh 180 -ab 4  ' + octree_fname + ' > ' + hdr_fname
            os.system(rpict)


    def path(folder, view, year, month, day):
        try:
            os.makedirs(os.path.join(folder, view, year, month, day))
        except OSError:
            pass
        return os.path.join(folder, view, year, month, day)


    def taskEC(viewtime, state, view):
        m = str(viewtime[0])
        d = str(viewtime[1])
        h = str(round(float(viewtime[2]), 2))
        dir = str(viewtime[3])
        diff = str(viewtime[4])
        year = str(viewtime[5])
        view = view

        gendaylit(m, d, h, dir, diff, year)

        genECHDR(m, d, h, view, state, year)


    def func(args):
        return taskEC(*args)


    pool = mp.Pool(processes=num_processes)
    for viewtime in viewtimes:
        for state in states:
            for view in views:
                comb = [viewtime, state, view]
                job_args.append(comb)
    pool.map(func, job_args)
    pool.close()
    pool.join()
