import os
import tarfile
import multiprocessing as mp

def extract_tar(tar_path, extract_path):
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(extract_path)

def extract_file(args):
    split, tar_file, root_dl_targz, root_dl = args
    tar_path = os.path.join(root_dl_targz, split, tar_file)
    tar_name = os.path.splitext(tar_file)[0]
    extract_path = os.path.join(root_dl, split)
    if not os.path.exists(extract_path):
        os.makedirs(extract_path, exist_ok=True)
    print(f'Extracting {tar_path} to {extract_path}')
    extract_tar(tar_path, extract_path)

if __name__ == "__main__":
    root_dl = 'k400'
    root_dl_targz = 'k400_targz'
    splits = ['train', 'val', 'test', 'replacement']

    if not os.path.exists(root_dl):
        os.makedirs(root_dl)

    num_cores = mp.cpu_count()

    for split in splits:
        args_list = []
        pool = mp.Pool(processes=num_cores//2)
        curr_dl = os.path.join(root_dl_targz, split)
        if split == 'replacement':
            tar_list = [tar_file for tar_file in os.listdir(curr_dl) if tar_file.endswith('.tgz')]
        else:
            tar_list = [tar_file for tar_file in os.listdir(curr_dl) if tar_file.endswith('.tar.gz')]
        args_list.extend([(split, tar_file, root_dl_targz, root_dl) for tar_file in tar_list])

        print(len(args_list), "total tar.gz files to extract")

        pool.map(extract_file, args_list)

        pool.close()
        pool.join()

        print("\nAll extractions complete!")