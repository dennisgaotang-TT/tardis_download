import json
import os
import stat
import re
import datetime


def get_config(file_path):
    return parse_json_file(file_path)

def parse_json_file(file_path):
    with open(file_path, "r") as fd:
        content = fd.read()
    return json.loads(content)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def ensure_dir(dirname: str, do_create=True):
    try:
        stat_buf = os.stat(dirname)
        if not stat.S_ISDIR(stat_buf.st_mode):
            raise FileExistsError("%s already exists and is not a directory" % dirname)
    except FileNotFoundError:
        if do_create:
            print("create dir: %s" % dirname)
            # os.mkdir(dirname)
            os.makedirs(dirname, exist_ok=True)

def get_file_num(dirname):
    files = [f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]
    return len(files)

def get_files(dirname, reverse=True):
    res = []
    for fn in os.listdir(dirname):
        fp = os.path.join(dirname, fn)
        if os.path.isfile(fp):
            res.append(fp)
    return sorted(res, reverse=reverse)

def get_dt_from_file_path(file_path):
    '''
    file name example: 2022-02-03T21:00:00.011Z.gz/2022-02-02.json.gz
    '''
    try:
        dt_str = os.path.splitext(os.path.basename(file_path))[0]
        dt = datetime.datetime.fromisoformat(
            dt_str.replace("Z", "+00:00")
        )
        return dt
    except ValueError:
        pass
    fn = os.path.basename(file_path)
    dt_str = os.path.splitext(fn)[0]
    match = re.search(r'(\d+\-\d+\-\d+)', fn)
    if match is None:
        raise Exception("parse file: %s error" % file_path)
    day_str = match.group(1)
    dt_str = "%sT00:00:00.000+00:00" % day_str
    return datetime.datetime.fromisoformat(dt_str)

def get_timestamp_str(float_seconds):
    dt = datetime.datetime.utcfromtimestamp(float_seconds)
    dt_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt_str

def get_day_ranges(start_day, end_day):
    start_dt = datetime.datetime.strptime(start_day, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_day, "%Y-%m-%d")
    cur_day = start_dt
    days = []
    while cur_day <= end_dt:
        days.append(cur_day)
        cur_day += datetime.timedelta(days=1)
    return days
