# coding=utf-8
import cx_Oracle


class test:
    def __init__(self):
        self.account_conn = None
        self.account_cur = None
        self.v_reson = ''

    def _init_db(self):
        try:
            user = 'accounting'
            password = 'D6$accounting'
            host = '172.32.148.119:1521'
            service_name = 'ngntdb2'
            conn_str = f"{user}/{password}@{host}/{service_name}"
            self.account_conn = cx_Oracle.connect(conn_str)
            self.account_cur = self.account_conn.cursor()
            print("初始化数据库完成")
        except Exception as e:
            print(f"初始化数据库异常_init_pub_db, 异常信息：[{e}]")
            raise e

    def _check_E03_R1001_L1016(self, i_msisdn):
        user_resource_sql = 'select count(1) from user_resource where resource_type = 1 and resource_value = :i_msisdn'
        param = {'i_msisdn': i_msisdn}
        user_resource_data = self.account_cur.execute(user_resource_sql, param).fetchone()
        n_total_cnt = user_resource_data[0]
        user_sql = "select count(1) from users where service_number =  :i_msisdn "
        param = {'i_msisdn': str(i_msisdn).replace('None', '0')}
        user_data = self.account_cur.execute(user_sql, param).fetchone()
        n_users_total_cnt = user_data[0]

        if n_users_total_cnt is None:
            n_users_total_cnt = 0
        if n_total_cnt == 0 and n_users_total_cnt == 0:
            self.v_reson = '系统中无资料'

    def _main_deal(self):
        self._init_db()
        print("进去查询")
        sql_error_cdr_analyse_detail_data = "SELECT CDR_DATE,MSISDN,OPERATION_ID,ORIG_ERROR_CODE FROM error_cdr_analyse_detail WHERE CDR_DATE = TO_NUMBER(TO_CHAR(SYSDATE-1, 'YYYYMMDD'))"
        print(f"执行sql ")
        self.account_cur.execute(sql_error_cdr_analyse_detail_data)
        error_cdr_analyse_detail_data = self.account_cur.fetchall()
        print("error_cdr_analyse_detail_data", error_cdr_analyse_detail_data)
        for item in error_cdr_analyse_detail_data:
            sql = "SELECT GRADE_ERROR_CODE,RESERVED2 FROM ERROR_MATRIX where ORIG_ERROR_CODE=:orig_error_code and RESERVED1=1"
            param = {'orig_error_code': item[3]}
            self.account_cur.execute(sql, param)
            error_matrix_data = self.account_cur.fetchone()
            # 进入逻辑分析
            try:
                self._check_E03_R1001_L1016(item[1])
            except Exception as e:
                print(f"异常 {e}， msisdn:{item[1]}")
                raise e
            if self.v_reson is not '':
                sql = 'update ERROR_CDR_ANALYSE_DETAIL set REMARK_BOSS=:remark_boss,GRADE_ERROR_CODE =:grade_error_code,PUSH_CHANNEL = :push_channel where OPERATION_ID = :operation_id'
                param = {'operation_id': item[2],
                         'remark_boss': self.v_reson,
                         'grade_error_code': error_matrix_data[0],
                         'push_channel': error_matrix_data[1]}
                self.account_cur.execute(sql, param)


if __name__ == '__main__':
    sp_error = test()
    try:
        sp_error._main_deal()
    except Exception as e:
        print(f"error ================{e}")
        raise e
