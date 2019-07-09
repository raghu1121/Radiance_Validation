import os

path = 'hdrs'
for root, d_names, f_names in os.walk(path):
    for f in f_names:
        filepath = os.path.join(root, f)
        size = os.path.getsize(filepath)
        if size < 500000:
            os.remove(filepath)