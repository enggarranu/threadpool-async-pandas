import math
from sqlalchemy import create_engine
import pandas as pd
import constant as c
from multiprocessing.pool import ThreadPool, cpu_count

#create connection using sqlalchemy connections string
engine_cloud = create_engine(c.cloud)
engine_production = create_engine(c.prod)

#declare the tables and primary keys to be used
tables = c.tables

#update pending transaction in cloud db
def update_pending_transactions():
    # getting pending transactions id
    sql = "select id_transaksi from \"TrTransaksi\" where \"StatusTRX\" = 'PENDING' order by 1;"
    df_pending = pd.read_sql_query(sql, engine_cloud)
    id_transaksi_arr = str(tuple(df_pending['id_transaksi']))
    print ("updateing %s transactions" %str(len(id_transaksi_arr)))

    # delete pending transaction by transaction id
    sql_delete_trstock = "delete from \"TrStock\" where id_transaksi in %s returning id_transaksi;" %id_transaksi_arr
    sql_delete_trtransaksi = "delete from \"TrTransaksi\" where id_transaksi in %s returning id_transaksi;" %id_transaksi_arr
    pd.read_sql_query(sql_delete_trstock,engine_cloud)
    pd.read_sql_query(sql_delete_trtransaksi, engine_cloud)

    # getting data on first table (trtransaki) and insert to cloud table
    sql_select = "select * from \"TrTransaksi\" where id_transaksi in %s;" %id_transaksi_arr
    df_transaksi = pd.read_sql_query(sql_select,engine_production)
    df_transaksi.to_sql(name='TrTransaksi', con=engine_cloud, if_exists='append', schema='public', index=False)

    # getting data on first table (trstock) and insert to cloud table
    sql_select_stock = "select * from \"TrStock\" where id_transaksi in %s;" %id_transaksi_arr
    df_trstock = pd.read_sql_query(sql_select_stock,engine_production)
    df_trstock.to_sql(name='TrStock', con=engine_cloud, if_exists='append', schema='public', index=False)

# function for insert data from local to cloud
def insert_data(tb_name, p_key, l, limit, offset):
    try:
        # print ("processing page " + str(i) + " from " + str(page))
        sql_select = "select * from \"%s\" where \"%s\" > %s order by \"%s\" asc limit %s offset %s;" % (
        tb_name, p_key, l, p_key, limit, offset)
        print (sql_select)
        df_select = pd.read_sql_query(sql_select, engine_production)
        df_select.to_sql(name=tb_name, con=engine_cloud, if_exists='append', schema='public', index=False)
        return 'ok'
    except Exception, e:
        print (e)
        return str(e)


def calc_pagenation(tb_name="", p_key=""):
    # getting latest primary key
    print ("processing: "+tb_name+" with pkey: "+p_key)
    sql_get_last_data = "select max(\"%s\") as \"l\" from \"%s\";" % (p_key, tb_name)
    df_sql_get_last_data = pd.read_sql_query(sql_get_last_data, engine_cloud)  # nanti diganti
    l = df_sql_get_last_data['l'][0]
    print (sql_get_last_data)
    print ("max p_key: "+ str(l))

    # counting row data
    sql_cloud_count_data = "select count(\"%s\") as \"c\" from \"%s\" where \"%s\" > %s;" % (p_key, tb_name, p_key, l)
    # sql_cloud_count_data = "select count(\"%s\") as \"c\" from \"%s\";"
    df_sql_cloud_c = pd.read_sql_query(sql_cloud_count_data, engine_production)
    c = df_sql_cloud_c['c'][0]
    print(sql_cloud_count_data)
    print ("len data: "+ str(c))

    # calculate offset
    offset_arr = []
    limit = 1000
    d = float(c) / limit
    page = int(math.ceil(d))
    print ("creating "+ str(page)+" page")
    for a in range(0,page):
        o = a*limit
        offset_arr.append(o)

    # create Threadpool by CPU Count for inserting data
    pool_pagenation = ThreadPool(cpu_count())
    results = []
    for offset in offset_arr:
        results.append(pool_pagenation.apply_async(insert_data, args=(tb_name, p_key, l, limit, offset)))
    pool_pagenation.close()
    pool_pagenation.join()
    results = [r.get() for r in results]
    print results

    print ("processing: " + tb_name + " with pkey: " + p_key + "COMPLETED")
    print ("="*100)

if __name__ == '__main__':
    print("updating pending transactions")
    update_pending_transactions()
    print("updating pending transactions [Complete]")
    print ("="*100)

    print ("Appending data")
    pool_table = ThreadPool(len(tables))
    result_table_pool = []
    for tb_name, p_key in tables:
        print("-"*100)
        result_table_pool.append(pool_table.apply_async(calc_pagenation, args=(tb_name, p_key)))
    pool_table.close()
    pool_table.join()
    result_table_pool = [res.get() for res in result_table_pool]
    print (result_table_pool)
    print("ALL DONE")
    #EOF