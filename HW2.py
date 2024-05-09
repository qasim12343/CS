import os

n = int(input())

path = input()
r = os.path.exists('dir')
if r:
    print(r)
    for i in range(n):
        new_path = os.path.join(path, f'folder{i}')
        os.mkdir(new_path)
