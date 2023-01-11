import requests
import json
import pandas as pd
import time


def read_last_line(filename):
    with open(filename) as f:
        for line in f:
            pass
        return line.strip()


def collect_result(df, url):
    resp = requests.get(url=url)
    resp_dict = json.loads(resp.text)
    new_row = pd.Series(resp_dict)
    df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    return resp_dict, df


def collect_mean_result(url):
    df = pd.DataFrame(columns=['elapseTime', 'latency', 'throughput'])
    for i in range(10):
        resp_dict, df = collect_result(df, url)
        print(resp_dict)
        time.sleep(1)
    return df.mean()


def get_data_filename(ip_file, config_file):
    num_nodes = sum(1 for _ in open(ip_file))
    with open(config_file) as f:
        data = json.load(f)
        # algorithm-num_nodes-num_byz_node-byz_strategy-block_size
        filename = "data/{}-{}-{}-{}-{}.csv".format(
            data['algorithm'], num_nodes, data['byzNo'], data['strategy'], data['block_size'])
        return filename


if __name__ == '__main__':
    ip_addr = read_last_line('public_ips.txt')
    url = "http://{}:8070/query".format(ip_addr)

    mean_result = collect_mean_result(url)
    mean_result.to_csv(get_data_filename('public_ips.txt', 'config.json'))

    print(mean_result)
