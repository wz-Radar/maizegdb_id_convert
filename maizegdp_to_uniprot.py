import bs4
import requests
from time import sleep
import time
import pandas as pd
import re

def mgdp_to_uni(gene_id):

    url1 = r"https://maizegdb.org/record_data/gene_data.php?id="
    url2 = r"&type=overview"
    url = url1 + gene_id + url2
    HEADER = {'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"}

    t_start = time.time()
    try:
        #timeout参数的两个元素分别是等待连接和读取的最大等待时间
        r = requests.get(url,headers = HEADER, timeout = (5, 7))
    except requests.exceptions.ReadTimeout:
        print("读取超时")
        return None
    except requests.exceptions.ConnectionError:
        print("连接超时")
        return None
    else:
        t_end = time.time()
        print("获取成功，用时：" + str(t_end - t_start) + " s")
        soup = bs4.BeautifulSoup(r.text, 'html.parser')
        soup_uni = soup.find(name = 'b', string = 'UniProt accession(s)')

        #获取失败，返回空列表
        if(soup_uni == None): 
            return []

        soup_f = soup_uni.find_next_siblings('a', string= re.compile('^([0-9A-Z]+)$'))
        uni_ids = []
        for i in soup_f:
            uni_ids.append(i.contents[0])
        return uni_ids

if __name__ == "__main__":
    df_mgdp = pd.read_csv(r"input.csv", dtype = str)
    id_json = []
    id_fail= []
    t_sum = 0

    for item in df_mgdp['gene_name']:

        #防止短时间太多访问把网站崩了
        sleep(0.5) 

        t_start = time.time()
        uni = mgdp_to_uni(item)
        t_end = time.time()
        t_sum += t_end - t_start
        if(uni == None):
            id_fail.append(item)
            continue
        else:
            uni = list(set(uni))
        id_json.append({'maizegdp_id': item,
                        'uniprot_id': uni})

    print(id_fail)
    df_id = pd.json_normalize(id_json)
    print(df_id)
    id_table = []
    for i in df_id.index:
        if(df_id.loc[i, 'uniprot_id'] == []):
            id_table.append([df_id.loc[i, 'maizegdp_id'], ' '])
            continue
        for j in df_id.loc[i, 'uniprot_id']:
            id_table.append([df_id.loc[i, 'maizegdp_id'], j])
    id_table = pd.DataFrame(id_table, columns = ['maizegdp_id', 'uniprot_id'])
    print(id_table)
    id_table.to_csv(r'output.csv', index=False)
    id_fail = pd.DataFrame(id_fail, columns=['gene_name'])
    #获取失败的基因id存入fail.csv文件中
    id_fail.to_csv(r'fail.csv', index=False)
    print(t_sum)